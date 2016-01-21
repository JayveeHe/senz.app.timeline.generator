import datetime
import pytz
from dao_utils import get_user_hos, get_user_events, get_mot_item, get_loc_item, save_timeline2mongo
import dao_utils
import time_utils

__author__ = 'jayvee'

start_timestamp = time_utils.trans_strtime2timestamp('2016-01-10 00:00:00')
end_timestamp = time_utils.trans_strtime2timestamp('2016-01-10 23:59:59')


def select_largest_key(prob_dict):
    largest_key = prob_dict.keys()[0]
    for tmp_key in prob_dict.keys():
        if prob_dict[tmp_key] > prob_dict[largest_key]:
            largest_key = tmp_key
    return largest_key


def get_nearest_poi(multi_poi):
    """
    get nearest poi from multi pois
    :param multi_poi:
    :return:
    """
    cur_poi = multi_poi[0]['raw_poi']['title']
    cur_dist = multi_poi[0]['raw_poi']['_distance']
    for poi in multi_poi:
        if poi['raw_poi']['_distance'] < cur_dist:
            cur_poi = poi['raw_poi']['title']
            cur_dist = poi['raw_poi']['_distance']
    return cur_poi, cur_dist


def combine_timeline(user_id, time_range):
    """
    :param user_id:
    :param time_range: 10 digits
    :return:
    """
    hos_list = get_user_hos(user_id, time_range)
    event_list = get_user_events(user_id, time_range)
    combine_list = []

    # level1
    # split by hos status
    if len(hos_list) > 0:
        last_hos_status = hos_list[0]['status']
        last_hos_timestamp = hos_list[0]['timestamp']
        split_hos_list = []
        for hos in hos_list:
            cur_hos_status = hos['status']
            cur_hos_timestamp = hos['timestamp']
            if cur_hos_status != last_hos_status and len(split_hos_list) > 1:
                # TODO smooth hos status
                combine_list.append({'type': 'hos', 'timestamp': split_hos_list[0]['timestamp'],
                                     'data': {'startTime': split_hos_list[0]['timestamp'],
                                              'endTime': split_hos_list[len(split_hos_list) - 1]['timestamp'],
                                              'status': split_hos_list[0]['status'],
                                              'user_location_id': split_hos_list[0]['user_location_id'],
                                              'start_location_id': split_hos_list[0]['user_location_id'],
                                              'end_location_id': split_hos_list[len(split_hos_list) - 1][
                                                  'user_location_id'],
                                              '_id': str(split_hos_list[0]['_id']),
                                              'hos_evidences': [str(x['_id']) for x in split_hos_list]}})
                split_hos_list = [hos]
                last_hos_status = hos['status']
            else:
                split_hos_list.append(hos)
            # last_hos_status = hos['status']
            last_hos_timestamp = hos['timestamp']
        # handle the last hos
        if len(split_hos_list) > 0:
            combine_list.append({'type': 'hos', 'timestamp': split_hos_list[0]['timestamp'],
                                 'data': {'startTime': split_hos_list[0]['timestamp'],
                                          'endTime': split_hos_list[len(split_hos_list) - 1]['timestamp'],
                                          'status': split_hos_list[0]['status'],
                                          'user_location_id': split_hos_list[0]['user_location_id'],
                                          'start_location_id': split_hos_list[0]['user_location_id'],
                                          'end_location_id': split_hos_list[len(split_hos_list) - 1][
                                              'user_location_id'],
                                          '_id': str(split_hos_list[0]['_id']),
                                          'hos_evidences': [str(x['_id']) for x in split_hos_list]}})
            # combine_list.append({'type': 'hos', 'timestamp': hos['timestamp'], 'data': hos})
    if len(event_list) > 0:
        split_event_list = []
        last_event_label = event_list[0]['event'].keys()[0]
        last_l2_event_label = event_list[0].get('level2_event', None)
        if not last_l2_event_label:
            event_list[0]['level2_event'] = ''
            last_l2_event_label = ''
        last_event = event_list[0]
        event_ids = []
        for event in event_list:
            # combine event list
            cur_event_label = event['event'].keys()[0]
            cur_l2_event_label = event.get('level2_event')
            if not cur_l2_event_label:
                cur_l2_event_label = ''
                event['level2_event'] = ''
            if last_event_label != cur_event_label or last_l2_event_label != cur_l2_event_label:
                # push split event list
                evidence_list = []
                for i_event in split_event_list:
                    evidence_list.extend(i_event['evidence_list'])
                last_event['start_location_id'] = evidence_list[0]['location_id']
                last_event['end_location_id'] = evidence_list[len(evidence_list) - 1]['location_id']
                last_event['startTime'] = split_event_list[0]['startTime']
                last_event['start_datetime'] = split_event_list[0]['start_datetime']
                last_event['timestamp'] = split_event_list[0]['timestamp']
                last_event['event_ids'] = event_ids
                combine_list.append({'type': 'event', 'timestamp': last_event['startTime'], 'data': last_event})
                # status reset
                split_event_list = [event]
                event_ids = [event['_id']]
                last_event_label = event['event'].keys()[0]
                last_l2_event_label = event['level2_event']
                last_event = event
            else:
                split_event_list.append(event)
                event_ids.append(event['_id'])

                # event['start_location_id'] = event['evidence_list'][0]['location_id']
                # event['end_location_id'] = event['evidence_list'][len(event['evidence_list']) - 1]['location_id']
                # combine_list.append({'type': 'event', 'timestamp': event['startTime'], 'data': event})
    sorted_list = sorted(combine_list, cmp=lambda x, y: cmp(x['timestamp'], y['timestamp']))
    timeline = []
    for item in sorted_list:
        start_location_item = get_loc_item(item['data']['start_location_id'])
        start_title_head = '%s%s%s ' % (
            start_location_item['city'], start_location_item['district'], start_location_item['street'])
        start_poi = get_nearest_poi(start_location_item['pois']['pois'])
        start_poi = (start_title_head + start_poi[0], start_poi[1])
        end_location_item = get_loc_item(item['data']['end_location_id'])
        end_title_head = '%s%s%s ' % (
            end_location_item['city'], end_location_item['district'], end_location_item['street'])
        end_poi = get_nearest_poi(end_location_item['pois']['pois'])
        end_poi = (end_title_head + end_poi[0], end_poi[1])

        if item['type'] == 'hos':
            poi = get_loc_item(item['data']['user_location_id'])
            title_head = '%s%s%s ' % (poi['city'], poi['district'], poi['street'])
            nearest_poi = get_nearest_poi(poi['pois']['pois'])
            nearest_poi = (title_head + nearest_poi[0], nearest_poi[1])
            # remove event that indicate hos at same time
            if len(timeline) > 0:
                hos_event_map = {'at_home': 'relaxing', 'at_office': 'work_in_office'}
                hos_label = item['data']['status']
                if timeline[-1]['label'] == hos_event_map.get(hos_label) and \
                                timeline[-1]['start_ts'] == item['data']['startTime']:
                    # timeline.remove(timeline[-1])
                    timeline[-1]['start_ts'] -= 10000
            timeline.append({'user_id': user_id, 'type': 'hos',
                             'label': item['data']['status'],
                             'timestamp': item['data']['startTime'],
                             'start_ts': item['data']['startTime'],
                             'start_datetime': datetime.datetime.fromtimestamp(item['data']['startTime'] / 1000,
                                                                               pytz.timezone('UTC')),
                             'end_ts': item['data']['endTime'],
                             'end_datetime': datetime.datetime.fromtimestamp(item['data']['endTime'] / 1000,
                                                                             pytz.timezone('UTC')),
                             'start_location': {'title': start_poi[0], 'dist': start_poi[1],
                                                'geo_point': start_location_item['location']},
                             'end_location': {'title': end_poi[0], 'dist': end_poi[1],
                                              'geo_point': end_location_item['location']},
                             'evidence_list': {
                                 'hos_ids': item['data']['hos_evidences']},
                             'motion_count': {},
                             'poi': {'title': nearest_poi[0], 'dist': nearest_poi[1], 'geo_point': poi['location']}})
        if item['type'] == 'event':
            poi = get_loc_item(
                item['data']['evidence_list'][int(len(item['data']['evidence_list']) / 2)]['location_id'])
            title_head = '%s%s%s ' % (poi['city'], poi['district'], poi['street'])
            nearest_poi = get_nearest_poi(poi['pois']['pois'])
            nearest_poi = (title_head + nearest_poi[0], nearest_poi[1])
            # nearest_poi = get_nearest_poi(poi['pois']['pois'])
            location_ids = [x['location_id'] for x in item['data']['evidence_list']]
            motion_ids = [x['motion_id'] for x in item['data']['evidence_list']
                          if x['motion_id']]
            # count motion types
            motion_dict = {}
            for motion_id in motion_ids:
                motion = get_mot_item(motion_id)
                if motion['motionProb']:
                    motion_type = select_largest_key(motion['motionProb'])
                    if motion_type in motion_dict:
                        motion_dict[motion_type] += 1
                    else:
                        motion_dict[motion_type] = 1
            event_label = item['data']['event'].keys()[0]
            # if event_label == 'going_out':
            #     if item['data']['isOnSubway']:
            #         event_label = 'on_subway'
            # remove event that indicate hos at same time
            if len(timeline) > 0:
                event_hos_map = {'relaxing': 'at_home', 'work_in_office': 'at_office'}
                hos_label = timeline[-1]['label']
                if event_label == event_hos_map.get(hos_label) \
                        and timeline[-1]['start_ts'] == item['data']['startTime']:
                    # continue
                    item['data']['startTime'] += 10000
            timeline.append(
                {'user_id': user_id, 'type': 'event',
                 'label': event_label,
                 'level2_event': item['data']['level2_event'],
                 'event_ids': item['data']['event_ids'],
                 'timestamp': item['data']['startTime'],
                 'start_ts': item['data']['startTime'],
                 'start_datetime': datetime.datetime.fromtimestamp(item['data']['startTime'] / 1000,
                                                                   pytz.timezone('UTC')),
                 'end_ts': item['data']['endTime'],
                 'end_datetime': datetime.datetime.fromtimestamp(item['data']['endTime'] / 1000, pytz.timezone('UTC')),
                 'start_location': {'title': start_poi[0], 'dist': start_poi[1],
                                    'geo_point': start_location_item['location']},
                 'end_location': {'title': end_poi[0], 'dist': end_poi[1],
                                  'geo_point': end_location_item['location']},
                 'evidence_list': {'location_ids': location_ids,
                                   'motion_ids': motion_ids, 'event_ids': [item['data']['_id']]},
                 'motion_count': motion_dict,
                 'poi': {'title': nearest_poi[0], 'dist': nearest_poi[1], 'geo_point': poi['location']}})
            # for
            # print '12312'
            # pass
    return timeline


if __name__ == '__main__':
    # get_user_hos('564ee2fbddb28e2d3f880165', (start_timestamp, end_timestamp))
    # get_user_events('560388c100b09b53b59504d2', (start_timestamp, end_timestamp))
    start_timestamp = time_utils.trans_strtime2timestamp('2016-01-10 00:00:00')
    end_timestamp = time_utils.trans_strtime2timestamp('2016-01-21 23:59:59')
    combined_timelines = combine_timeline('5634da2360b22ab52ef82a45', (start_timestamp, end_timestamp))
    if dao_utils.save_raw_timeline2mongo(combined_timelines):
        print 'done'
    else:
        print 'error'
