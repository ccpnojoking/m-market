import os
import sys
import datetime
import pandas as pd

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR + '/../utils')
sys.path.append(DIR + '/../apis')

from apis.m_api import MApi
from utils.logger_util import Logger


class MMarket(MApi):

    def __init__(self, app):
        super().__init__(app)
        self.app = app
        self.logger = Logger('M Market Job', os.path.join(DIR, 'logs/m_market_job.log'))

    def is_path_existed(self, path):
        if os.path.exists(os.path.dirname(path)):
            return True
        self.logger.info(f'not exist {path}')
        os.makedirs(os.path.dirname(path))

    def format_payer_count(self, date):
        payer_count = self.get_payer_count(date)
        if not payer_count.get('opratorList'):
            self.logger.error('get payer_count_info failed')
            return None
        operator_pay_list = payer_count['opratorList']
        web_pay_list = payer_count['webpayList']
        format_payer_count = {}
        for operator_pay in operator_pay_list:
            ad_id = operator_pay['adid'] if operator_pay['adid'] else 0
            channel_name = operator_pay['channel_name']
            format_payer_count[f"{channel_name}-{ad_id}"] = {
                'channel_name': channel_name,
                'ad_id': ad_id,
                'operator_pay': operator_pay['count']
            }
        for web_pay in web_pay_list:
            ad_id = web_pay['adid'] if web_pay['adid'] else 0
            channel_name = web_pay['channel_name']
            key = f"{channel_name}-{ad_id}"
            if format_payer_count.get(key):
                format_payer_count[key]['web_pay'] = web_pay['count']
                continue
            format_payer_count[key] = {
                'channel_name': channel_name,
                'ad_id': ad_id,
                'web_pay': web_pay['count']
            }
        return format_payer_count

    def get_download_payer_count(self, date, hour):
        payer_count = self.format_payer_count(date)
        if not payer_count:
            return None
        new_payer_count = {
            'date': [],
            'hour': [],
            'channel_name': [],
            'ad_id': [],
            'operator_pay': [],
            'web_pay': [],
            'pay_count': [],
        }
        for channel_and_ad, payer_info in payer_count.items():
            operator_pay = payer_info.get('operator_pay', 0)
            web_pay = payer_info.get('web_pay', 0)
            new_payer_count['channel_name'].append(payer_info['channel_name'])
            new_payer_count['ad_id'].append(int(payer_info['ad_id']))
            new_payer_count['operator_pay'].append(operator_pay)
            new_payer_count['web_pay'].append(web_pay)
            new_payer_count['pay_count'].append(operator_pay + web_pay)
            new_payer_count['date'].append(date)
            new_payer_count['hour'].append(hour)
        new_payer_count = pd.DataFrame(new_payer_count)
        new_payer_count.sort_values(['channel_name', 'pay_count'], ascending=[False, False], inplace=True)
        return new_payer_count

    def download_now_time_payer_count(self):
        datetime_now = datetime.datetime.now()
        now_date = datetime.datetime.strftime(datetime_now, '%Y-%m-%d')
        now_hour = datetime_now.hour
        payer_count = self.get_download_payer_count(now_date, now_hour)
        payer_count_path = os.path.join(DIR, f'files/m_market_files/m_{now_date}_{now_hour}_{self.app}.csv')
        self.is_path_existed(payer_count_path)
        payer_count.to_csv(payer_count_path, index=False)

    def download_any_time_payer_count(self, date, hour=24):
        payer_count = self.get_download_payer_count(date, hour)
        payer_count_path = os.path.join(DIR, f'files/m_market_files/m_{date}_{hour}_{self.app}.csv')
        self.is_path_existed(payer_count_path)
        payer_count.to_csv(payer_count_path, index=False)


def main():
    job = MMarket('com.laidian.xiu')

    # now time
    job.download_now_time_payer_count()

    # any time
    date_list = [
    ]
    for date in date_list:
        job.download_any_time_payer_count(date)


if __name__ == '__main__':
    main()
