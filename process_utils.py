import datetime, time
import gevent
from dao_utils import db_combined_timeline
import dao_utils
from data_collector import combine_timeline

__author__ = 'jayvee'


def process_timeline(user_id, time_range, tag=''):
    """
    process single user's timeline with timerange
    :param user_id:
    :param time_range: 10 digits
    :return:
    """
    print '[%s]start process user:%s time_range: from %s to %s' % (
        datetime.datetime.now(), user_id, datetime.datetime.fromtimestamp(time_range[0]),
        datetime.datetime.fromtimestamp(time_range[1]))
    combined_timelines = combine_timeline(user_id, (time_range[0], time_range[1]))
    # insert_count = save_timeline2mongo(combined_timelines)
    insert_count = dao_utils.save_raw_timeline2mongo(combined_timelines, tag=tag)
    if insert_count is not None:
        print '[%s]user:%s done, process timeline count = %s' % (datetime.datetime.now(), user_id, insert_count)
    else:
        print '[%s]user:%s error' % (datetime.datetime.now(), user_id)


def process_all_timelines(time_range, tag='', is_offline=False):
    start_time = time.time()
    print 'start trigger at %s' % (start_time)
    user_ids = dao_utils.get_all_users()
    # for user_id in user_ids:
    #     process_timeline(user_id, time_range)
    # print 'process all timelines done at %s' % (datetime.datetime.now())
    gevent_task = []
    window = 20
    count = 0
    for user_id in user_ids:
        # query the latest event time
        tmp = db_combined_timeline.find({'user_id': user_id}).sort('timestamp', -1).limit(1)
        tmp_item = next(tmp, None)
        if is_offline:
            tmp_time_range = (time_range[0], time_range[1])
        else:
            if tmp_item:
                tmp_time_range = (tmp_item['timestamp'] / 1000, time_range[1])
            else:
                tmp_time_range = (time_range[0], time_range[1])

        gevent_task.append(gevent.spawn(process_timeline, user_id, tmp_time_range, tag))
        if count % window == 0:
            # print 'start gevent'
            gevent.joinall(gevent_task)
            gevent_task = []
        count += 1
    print 'process user timelines done! using time: %s, from %s to %s' % (
        (time.time() - start_time), datetime.datetime.fromtimestamp(start_time),
        datetime.datetime.fromtimestamp(time.time()))
    # logger.info('process user event done! using time: %s, from %s to %s' % (
    #     (time.time() - start_time), datetime.fromtimestamp(start_time), datetime.fromtimestamp(time.time())))
