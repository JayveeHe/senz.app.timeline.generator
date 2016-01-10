import json
import time
import datetime
import arrow
import requests

__author__ = 'jayvee'


def trans_strtime2timestamp(str_time='2015-11-19 18:23:00'):
    timestamp = time.mktime(datetime.datetime.strptime(str_time, '%Y-%m-%d %H:%M:%S').timetuple())
    return timestamp


def get_workday(date_str=str(arrow.now().strftime('%Y-%m-%d'))):
    for i in xrange(3):
        try:
            post_url = 'http://api.trysenz.com/v2/parserhub/api/workday'
            post_json = {
                "date": "%s" % date_str
            }
            date_result = requests.post(post_url, json=post_json)
            date_json = json.loads(date_result.text)
            if date_json.get('code') == 0:
                workday = date_json['result'].values()[0]
            else:
                workday = None
            return workday
        except requests.exceptions.Timeout, to:
            continue
        except Exception, e:
            continue
    return None


if __name__ == '__main__':
    pass
    # print get_holiday('2015-10-02')
