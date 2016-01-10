from dao_utils import get_user_hos, get_user_events, get_mot_item, get_loc_item, save_timeline2mongo
import time_utils

__author__ = 'jayvee'

start_timestamp = time_utils.trans_strtime2timestamp('2016-01-10 22:00:00')
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
        split_hos_list = [hos_list[0]]
        for hos in hos_list:
            cur_hos_status = hos['status']
            cur_hos_timestamp = hos['timestamp']
            if cur_hos_status != last_hos_status and len(split_hos_list) > 1:
                combine_list.append({'type': 'hos', 'timestamp': split_hos_list[0]['timestamp'],
                                     'data': {'startTime': split_hos_list[0]['timestamp'],
                                              'endTime': split_hos_list[len(split_hos_list) - 1]['timestamp'],
                                              'status': split_hos_list[0]['status'],
                                              'user_location_id': split_hos_list[0]['user_location_id'],
                                              '_id': str(split_hos_list[0]['_id']),
                                              'hos_evidences': [str(x['_id']) for x in split_hos_list]}})
                split_hos_list = [hos]
            else:
                split_hos_list.append(hos)
            last_hos_status = hos['status']
            last_hos_timestamp = hos['timestamp']
        # handle the last hos
        if len(split_hos_list) > 0:
            combine_list.append({'type': 'hos', 'timestamp': split_hos_list[0]['timestamp'],
                                 'data': {'startTime': split_hos_list[0]['timestamp'],
                                          'endTime': split_hos_list[len(split_hos_list) - 1]['timestamp'],
                                          'status': split_hos_list[0]['status'],
                                          'user_location_id': split_hos_list[0]['user_location_id'],
                                          '_id': str(split_hos_list[0]['_id']),
                                          'hos_evidences': [str(x['_id']) for x in split_hos_list]}})
            # combine_list.append({'type': 'hos', 'timestamp': hos['timestamp'], 'data': hos})
    if len(event_list) > 0:
        for event in event_list:
            combine_list.append({'type': 'event', 'timestamp': event['startTime'], 'data': event})
    sorted_list = sorted(combine_list, cmp=lambda x, y: cmp(x['timestamp'], y['timestamp']))
    timeline = []
    for item in sorted_list:
        if item['type'] == 'hos':
            poi = get_loc_item(item['data']['user_location_id'])
            nearest_poi = get_nearest_poi(poi['pois']['pois'])
            timeline.append({'user_id': user_id, 'type': 'hos',
                             'label': item['data']['status'],
                             'timestamp': item['data']['startTime'],
                             'evidence_list': {
                                 'hos_ids': item['data']['hos_evidences'],
                                 'location_ids': [],
                                 'motion_ids': [], 'event_ids': []},
                             'motion_count': {},
                             'poi': {'title': nearest_poi[0], 'dist': nearest_poi[1], 'geo_point': poi['location']}})
        if item['type'] == 'event':
            poi = get_loc_item(
                item['data']['evidence_list'][int(len(item['data']['evidence_list']) / 2)]['location_id'])
            nearest_poi = get_nearest_poi(poi['pois']['pois'])
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
            if event_label == 'going_out':
                if item['data']['isOnSubway']:
                    event_label = 'on_subway'
            timeline.append(
                {'user_id': user_id, 'type': 'event', 'label': event_label,
                 'timestamp': item['data']['startTime'],
                 'start_ts': item['data']['startTime'],
                 'end_ts': item['data']['endTime'],
                 'evidence_list': {'hos_ids': [], 'location_ids': location_ids,
                                   'motion_ids': motion_ids, 'event_ids': [item['data']['_id']]},
                 'motion_count': motion_dict,
                 'poi': {'title': nearest_poi[0], 'dist': nearest_poi[1], 'geo_point': poi['location']}})
            # for
            # print '12312'
            # pass
    return timeline


if __name__ == '__main__':
    # get_user_hos('564ee2fbddb28e2d3f880165', (start_timestamp, end_timestamp))
    # get_user_events('564ee2fbddb28e2d3f880165', (start_timestamp, end_timestamp))
    combined_timelines = combine_timeline('564bd84b60b2ed362064985f', (start_timestamp, end_timestamp))
    if save_timeline2mongo(combined_timelines):
        print 'done'
    else:
        print 'error'
