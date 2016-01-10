import datetime
import time
import gevent
from dao_utils import save_timeline2mongo, get_all_users, db_combined_timeline
from data_collector import combine_timeline

__author__ = 'jayvee'


def process_timeline(user_id, time_range):
    """
    process single user's timeline with timerange
    :param user_id:
    :param time_range: 10 digits
    :return:
    """
    combined_timelines = combine_timeline(user_id, (time_range[0], time_range[1]))
    insert_count = save_timeline2mongo(combined_timelines)
    if insert_count is not None:
        print 'user:%s done, process timeline count = %s' % (user_id, insert_count)
    else:
        print 'user:%s error' % user_id


def process_all_timelines(time_range):
    start_time = time.time()
    print 'start trigger at %s' % (start_time)
    user_ids = get_all_users()
    # for user_id in user_ids:
    #     process_timeline(user_id, time_range)
    # print 'process all timelines done at %s' % (datetime.datetime.now())
    gevent_task = []
    window = 20
    count = 0
    for user_id in user_ids:
        # query the latest event time
        tmp = db_combined_timeline.find({'user_id': user_id}).sort('timestamp', -1).limit(1)
        # tmp_item = next(tmp)
        for tmp_item in tmp:
            if tmp_item:
                time_range = (tmp_item['timestamp'] / 1000, time_range[1])

        gevent_task.append(gevent.spawn(process_timeline, user_id, time_range))
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


if __name__ == '__main__':
    current_time = time.time()
    process_all_timelines(time_range=((current_time - 24 * 3600.0), current_time))
    # process_timeline('560388c100b09b53b59504d2',time_range=((current_time - 6 * 3600.0), current_time))
