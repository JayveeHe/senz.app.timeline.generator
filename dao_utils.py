import copy
from bson import ObjectId
import datetime
import leancloud
import pymongo
import time_utils

__author__ = 'jayvee'

# senz.datasource.timeline key
leancloud.init('pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81',
               'qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia')
# av_hos = leancloud.Object.extend('')
av_events = leancloud.Object.extend('UserEvent')

# mongo
mongo_client = pymongo.MongoClient('119.254.111.40', 27017)
db_refinedlog = mongo_client.get_database('RefinedLog')
db_refinedlog.authenticate('senzhub', 'Senz2everyone')
db_user_location = db_refinedlog.get_collection('UserLocation')
db_user_motion = db_refinedlog.get_collection('UserMotion')
db_hos = db_refinedlog.get_collection('HomeOfficeStatus')
db_combined_timeline = db_refinedlog.get_collection('CombinedTimelines')


def get_all_users():
    user_query = leancloud.Query(leancloud.User)
    find_result = _find(user_query)
    user_ids = []
    for item in find_result:
        user_ids.append(item.id)
    return user_ids


def _find(query, max_num=None):
    """
    fetch all data from leancloud
    :param query:
    :param max_num:
    :return:
    """
    # fetch data from leancloud
    result = []
    if max_num is not None and max_num < 1000:
        _query = copy.deepcopy(query)
        _query.limit(max_num)
        return _query.find()
    count = query.count()
    pages = count / 1000 + 1
    # print count
    query_count = 0
    for i in range(pages):
        _query = copy.deepcopy(query)
        _query.limit(1000)
        _query.skip(i * 1000)
        res = _query.find()
        for item in res:
            if max_num and query_count >= max_num:
                return result
            result.append(item)
            query_count += 1
    return result


def get_user_hos(user_id, time_range):
    find_cursor = db_hos.find(
        {"user_id": user_id
            , "timestamp": {"$gt": time_range[0] * 1000, "$lt": time_range[1] * 1000}
         }).sort('timestamp', 1)
    user_hos_list = []
    for item in find_cursor:
        user_hos_list.append(item)

    return user_hos_list


def get_user_events(user_id, time_range):
    user_query = leancloud.Query(leancloud.User)
    user_result = user_query.equal_to('objectId', user_id).first()
    event_query = leancloud.Query(av_events)
    event_query.equal_to('user', user_result)
    event_query.greater_than_or_equal_to('startTime', time_range[0] * 1000)
    event_query.less_than_or_equal_to('endTime', time_range[1] * 1000)
    event_query.ascending('startTime')
    find_result = event_query.find()
    user_event_list = []
    for item in find_result:
        item.attributes['_id'] = item.id
        user_event_list.append(item.attributes)

    return user_event_list


def get_loc_item(loc_id):
    find_cursor = db_user_location.find({'_id': ObjectId(loc_id)})
    return next(find_cursor)


def get_mot_item(mot_id):
    find_cursor = db_user_motion.find({'_id': ObjectId(mot_id)})
    return next(find_cursor)


def save_raw_timeline2mongo(timeline_list, tag=''):
    try:
        insert_count = 0
        for timeline_item in timeline_list:
            timeline_item['tag'] = tag

            # check if exists
            user_id = timeline_item['user_id']
            start_ts = timeline_item['start_ts']
            end_ts = timeline_item['end_ts']
            item_type = timeline_item['type']
            find_result = db_combined_timeline.find_one({"user_id": user_id, 'start_ts': start_ts, 'type': item_type})
            if find_result:
                # todo what if timerange has changed?
                # if find_result['end_ts']:

                # find_result['updatedAt'] = datetime.datetime.utcnow()
                timeline_item['updatedAt'] = datetime.datetime.utcnow()
                db_combined_timeline.update({"user_id": user_id, 'start_ts': start_ts, 'type': item_type},
                                            timeline_item)
            else:
                timeline_item['createdAt'] = datetime.datetime.utcnow()
                timeline_item['updatedAt'] = datetime.datetime.utcnow()
                db_combined_timeline.insert(timeline_item)
            insert_count += 1
        return insert_count
    except Exception, e:
        print e
        return None


def save_timeline2mongo(timeline_list):
    try:
        insert_count = 0
        timeline_obj = {}
        for timeline_item in timeline_list:
            if timeline_item['type'] == 'hos':
                # start of a timeline obj
                if len(timeline_obj) > 0:
                    # save the last timeline_obj
                    timeline_obj['end_hos'] = timeline_item['label']
                    timeline_obj['end_ts'] = timeline_item['timestamp']
                    timeline_obj['end_datetime'] = timeline_item['start_datetime']
                    timeline_obj['end_location'] = timeline_item['poi']
                    timeline_obj['end_hos_evidences'] = timeline_item['evidence_list']['hos_ids']
                    timeline_obj['createdAt'] = datetime.datetime.utcnow()
                    db_combined_timeline.insert(timeline_obj)
                    insert_count += 1.0
                    # creat new timeline_obj
                    timeline_obj = {}
                timeline_obj['user_id'] = timeline_item['user_id']
                timeline_obj['cur_version'] = '0.1.0'
                timeline_obj['start_hos_evidences'] = timeline_item['evidence_list']['hos_ids']
                timeline_obj['start_hos'] = timeline_item['label']
                timeline_obj['start_ts'] = timeline_item['timestamp']
                timeline_obj['start_datetime'] = timeline_item['start_datetime']
                timeline_obj['start_location'] = timeline_item['poi']
                timeline_obj['event_timeline'] = []

            elif len(timeline_obj) > 0:
                timeline_obj['event_timeline'].append(timeline_item)

                # db_combined_timeline.insert(timeline_item)
                # insert_count += 1
        return insert_count
    except Exception, e:
        print e
        return None
