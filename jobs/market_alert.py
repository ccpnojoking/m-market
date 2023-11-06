import os
import sys
import datetime
import pandas as pd

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR)

from utils.dingtalk_util import DingTalk
from utils.config_util import CONFIG
from utils.logger_util import Logger

CHANNEL_TARGET = {
    'oppo': 'OPPO软件商店',
    'vivo': '步步高vivo'
}
ALERT_TARGET = {
    'com.ld.ring': {
        'price_limit': 35,
        'cost_limit': 500
    },
    'com.laidian.xiu': {
        'price_limit': 30,
        'cost_limit': 300
    }
}

class MarketAlert(DingTalk):

    def __init__(self):
        access_token = CONFIG['dingtalk']['market_monitor_access_token']
        secret = CONFIG['dingtalk']['market_monitor_secret']
        self.logger = Logger('Market Alert Job', os.path.join(DIR, 'logs/market_alert_job.log'))
        super().__init__(access_token, secret, self.logger)

    def run(self, app, channel):
        channel = CHANNEL_TARGET[channel]
        datetime_now = datetime.datetime.now()
        now_date = datetime.datetime.strftime(datetime_now, '%Y-%m-%d')
        now_hour = datetime_now.hour
        file_path = os.path.join(DIR, f'files/{channel}_market_files/{channel}_{now_date}_{now_hour}_{app}.csv')
        print(file_path)
        if not os.path.exists(file_path):
            # self.alert(f'## 监控脚本未执行 \n**{channel} {app}**')
            return None
        market_data = pd.read_csv(file_path)
        cost_alert = f"## {channel} {app}\n"
        price_alert = f"## 成本过高计划 ⚠️注意调整⚠️\n **{channel}  {app}**\n"
        for index, row in market_data.iterrows():
            if row['creative_id'] == '总计':
                cost_alert += f'会员新增成本: {round(row["pay_price"], 2)}, 消耗: {round(row["cost"], 2)}.'
                continue
            if row['pay_price'] < ALERT_TARGET[app]['price_limit'] or row['cost'] < ALERT_TARGET[app]['cost_limit']:
                continue
            price_alert += f'>计划: {row["plan_name"]} \n广告创意ID: {row["creative_id"]}\n\n' \
                           f'**会员新增成本: {round(row["pay_price"], 2)}, 消耗: {round(row["cost"], 2)}**\n\n'
        self.logger.info(cost_alert)
        # self.alert(cost_alert)
        self.logger.info(price_alert)
        # self.alert(price_alert)


def main():
    alert_job = MarketAlert()
    alert_job.run('com.laidian.xiu', 'vivo')


if __name__ == '__main__':
    main()






