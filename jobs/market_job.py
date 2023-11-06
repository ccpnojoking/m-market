import os
import sys
import datetime
import pandas as pd

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR + '/../utils')
sys.path.append(DIR + '/../apis')

from utils.logger_util import Logger
from apis.oppo_api import OPPOApi
from apis.vivo_api import VIVOApi


class MarketJob:

    def __init__(self, app, logger=None):
        self.channel_name = None
        self.app = app
        self.logger = logger if logger else Logger('Market Job', os.path.join(DIR, 'logs/market_job.log'))

    def is_path_existed(self, path):
        if os.path.exists(os.path.dirname(path)):
            return True
        self.logger.info(f'not exist {path}')
        os.makedirs(os.path.dirname(path))

    @staticmethod
    def format_creative_info(
            plan_id,
            plan_name,
            creative_id,
            cost,
            show_count,
            download_count,
            postback_active_count,
            postback_retain_count,
            postback_register_count,
            postback_pay_count
    ):
        return {
            'plan_id': plan_id,
            'plan_name': plan_name,
            'creative_id': creative_id,
            'cost': cost,
            'show_count': show_count,
            'download_count': download_count,
            'postback_active_count': postback_active_count,
            'postback_retain_count': postback_retain_count,
            'postback_register_count': postback_register_count,
            'postback_pay_count': postback_pay_count,
        }

    @staticmethod
    def get_market_keys():
        market_keys = [
            'date',
            'hour',
            'channel_name',
            'creative_id',
            'plan_id',
            'plan_name',
            'cost',
            'operator_pay',
            'web_pay',
            'pay_count',
            'pay_price',
            'gap_between_pay_count_and_postback_pay_count',
            'show_count',
            'download_count',
            'postback_active_count',
            'postback_retain_count',
            'postback_register_count',
            'postback_pay_count',
            'postback_active_price',
            'postback_retain_price',
            'postback_register_price',
            'postback_pay_price'
        ]
        return {key: [] for key in market_keys}

    @staticmethod
    def get_price(cost, amount):
        return round(cost / amount if amount else cost, 2)

    def insert_market_detail(self, market_info, creative_info):
        market_info['date'].append(creative_info['date'])
        market_info['hour'].append(creative_info['hour'])
        market_info['channel_name'].append(creative_info['channel_name'])
        market_info['creative_id'].append(creative_info['creative_id'])
        market_info['plan_id'].append(creative_info['plan_id'])
        market_info['plan_name'].append(creative_info['plan_name'])
        market_info['cost'].append(creative_info['cost'])
        market_info['operator_pay'].append(creative_info['operator_pay'])
        market_info['web_pay'].append(creative_info['web_pay'])
        market_info['pay_count'].append(creative_info['pay_count'])
        market_info['pay_price'].append(self.get_price(creative_info['cost'], creative_info['pay_count']))
        market_info['gap_between_pay_count_and_postback_pay_count'].append(
            creative_info['pay_count'] - creative_info['postback_pay_count']
        )
        market_info['show_count'].append(creative_info['show_count'])
        market_info['download_count'].append(creative_info['download_count'])
        market_info['postback_active_count'].append(creative_info['postback_active_count'])
        market_info['postback_retain_count'].append(creative_info['postback_retain_count'])
        market_info['postback_register_count'].append(creative_info['postback_register_count'])
        market_info['postback_pay_count'].append(creative_info['postback_pay_count'])
        market_info['postback_active_price'].append(
            self.get_price(creative_info['cost'], creative_info['postback_active_count'])
        )
        market_info['postback_retain_price'].append(
            self.get_price(creative_info['cost'], creative_info['postback_retain_count'])
        )
        market_info['postback_register_price'].append(
            self.get_price(creative_info['cost'], creative_info['postback_register_count'])
        )
        market_info['postback_pay_price'].append(
            self.get_price(creative_info['cost'], creative_info['postback_pay_count'])
        )
        return market_info

    def get_creative_info(self, date, creative_id):
        return {}

    def get_market_dataframe(self, m_market_dataframe, date, hour):
        market_info = self.get_market_keys()
        for index, row in m_market_dataframe.iterrows():
            if not all([row['channel_name'] == self.channel_name, row['ad_id']]):
                continue
            creative_info = self.get_creative_info(date, row['ad_id'])
            if not creative_info:
                continue
            creative_info['date'] = date
            creative_info['hour'] = hour
            creative_info['channel_name'] = self.channel_name
            creative_info['creative_id'] = row['ad_id']
            creative_info['operator_pay'] = row['operator_pay']
            creative_info['web_pay'] = row['web_pay']
            creative_info['pay_count'] = row['pay_count']
            market_info = self.insert_market_detail(market_info, creative_info)
        amount_info = {
            'date': date,
            'hour': hour,
            'channel_name': self.channel_name,
            'creative_id': '总计',
            'plan_id': '',
            'plan_name': '',
            'cost': sum(market_info['cost']),
            'show_count': sum(market_info['show_count']),
            'download_count': sum(market_info['download_count']),
            'operator_pay': sum(market_info['operator_pay']),
            'web_pay': sum(market_info['web_pay']),
            'pay_count': sum(market_info['pay_count']),
            'postback_active_count': sum(market_info['postback_active_count']),
            'postback_retain_count': sum(market_info['postback_retain_count']),
            'postback_register_count': sum(market_info['postback_register_count']),
            'postback_pay_count': sum(market_info['postback_pay_count'])
        }
        market_info = self.insert_market_detail(market_info, amount_info)
        market_data_df = pd.DataFrame(market_info)
        market_data_df.sort_values(['plan_id', 'creative_id'], ascending=[False, False], inplace=True)
        return market_data_df

    def download_now_time_market(self):
        datetime_now = datetime.datetime.now()
        now_date = datetime.datetime.strftime(datetime_now, '%Y-%m-%d')
        now_hour = datetime_now.hour
        m_market_path = os.path.join(DIR, f'files/m_market_files/m_{now_date}_{now_hour}_{self.app}.csv')
        if not os.path.exists(m_market_path):
            self.logger.error(f'not exist m_{now_date}_{now_hour}_{self.app}.csv')
            return None
        m_market_dataframe = pd.read_csv(m_market_path)
        market_dataframe = self.get_market_dataframe(m_market_dataframe, now_date, now_hour)
        self.logger.info(market_dataframe)
        channel_market_path = os.path.join(
            DIR, f'files/{self.channel_name}_market_files/{self.channel_name}_{now_date}_{now_hour}_{self.app}.csv')
        self.is_path_existed(channel_market_path)
        market_dataframe.to_csv(channel_market_path, index=False)

    def download_any_time_market(self, date, hour=24):
        m_market_path = os.path.join(DIR, f'files/m_market_files/m_{date}_{hour}_{self.app}.csv')
        if not os.path.exists(m_market_path):
            self.logger.error(f'not exist m_{date}_{hour}_{self.app}.csv')
            return None
        m_market_dataframe = pd.read_csv(m_market_path)
        market_data_df = self.get_market_dataframe(m_market_dataframe, date, hour)
        channel_market_path = os.path.join(
            DIR, f'files/{self.channel_name}_market_files/{self.channel_name}_{date}_{hour}_{self.app}.csv')
        self.is_path_existed(channel_market_path)
        market_data_df.to_csv(channel_market_path, index=False)


class OPPOMarketAnalysisa(MarketJob, OPPOApi):

    def __init__(self, app, logger=None):
        MarketJob.__init__(self, app, logger)
        OPPOApi.__init__(self, self.app, self.logger)
        self.channel_name = 'OPPO软件商店'

    def get_creative_info(self, date, creative_id):
        creative_info = self.get_single_creative_info(date, creative_id)
        if not creative_info.get('data'):
            return None
        items = creative_info['data']['items']
        if len(items) != 1:
            self.logger.error(f'items length: {len(items)}, date: {date}, creative_id: {creative_id}')
            return None
        creative_info = self.format_creative_info(
            plan_id=items[0]['plan_id'],
            plan_name=items[0]['plan_name'],
            creative_id=creative_id,
            cost=items[0]['cost']/100,
            show_count=items[0]['expose'],
            download_count=items[0]['download'],
            postback_active_count=items[0]['convert_active'],
            postback_retain_count=items[0]['convert_retention'],
            postback_register_count=items[0]['convert_register'],
            postback_pay_count=items[0]['convert_app_pay']
        )
        return creative_info


class VIVOMarketAnalysisa(MarketJob, VIVOApi):

    def __init__(self, app, logger=None):
        MarketJob.__init__(self, app, logger)
        VIVOApi.__init__(self, self.app, self.logger)
        self.channel_name = '步步高vivo'

    def get_creative_info(self, date, creative_id):
        creative_info = self.get_single_creative_info(date, date, creative_id)
        if not creative_info.get('data'):
            return None
        items = creative_info['data']['items']
        creative_info = self.format_creative_info(
            plan_id=items[0]['campaignId'],
            plan_name=items[0]['campaignName'],
            creative_id=creative_id,
            cost=sum([item['spent'] for item in items]),
            show_count=sum([item['showCount'] for item in items]),
            download_count=sum([item['downloadCount'] for item in items]),
            postback_active_count=sum([item['backActivateCount'] for item in items]),
            postback_retain_count=sum([item['customRetainCount'] for item in items]),
            postback_register_count=sum([item['backRegisterCount'] for item in items]),
            postback_pay_count=sum([item['customPayCount'] for item in items])
        )
        return creative_info


def main():
    # oppo_job = OPPOMarketAnalysisa('com.ld.ring')
    # oppo_job.download_now_time_market()
    vivo_job = VIVOMarketAnalysisa('com.laidian.xiu')
    vivo_job.download_now_time_market()


if __name__ == '__main__':
    main()
