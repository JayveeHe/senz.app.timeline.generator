import time
from process_utils import process_all_timelines

__author__ = 'jayvee'

if __name__ == '__main__':
    current_time = time.time()
    process_all_timelines(time_range=((current_time - 11*24 * 3600.0), current_time), tag='offline', is_offline=True)
    # process_timeline('560388c100b09b53b59504d2',time_range=((current_time - 24 * 3600.0), current_time))
