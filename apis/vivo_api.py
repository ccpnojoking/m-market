import requests
import time
import random
import hashlib
import os
import sys

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR + '/../utils')

from utils.config_util import CONFIG
from utils.logger_util import Logger


class VIVOApi:

    def __init__(self, app, logger=None):
        super().__init__()
        self.__access_token = CONFIG['vivo'][app]
        self.logger = logger if logger else Logger('VIVO Api', os.path.join(DIR, 'logs/vivo_api.log'))

    def get_single_creative_info(self, start_date, end_date, creative_id, summary_type='SUMMARY'):
        """
        :param start_date: '2023-10-25'
        :param end_date: '2023-10-26'
        """
        url = f"https://marketing-api.vivo.com.cn/openapi/v1/adstatement/summary/query?" \
              f"access_token={self.__access_token}" \
              f"&timestamp={int(time.time() * 1000)}" \
              f"&nonce={hashlib.md5(str(time.time() * random.randint(1, 1000)).encode()).hexdigest()}"
        request_body = {
            'startDate': start_date,
            'endDate': end_date,
            'pageSize': 20,
            'summaryType': summary_type,
            'filterFieldIds': {'creativeIds': [creative_id]}

        }
        response = requests.post(url, json=request_body).json()
        if not response['data']:
            self.logger.error(request_body)
            self.logger.error(response)
            return {}
        return response

    def get_all_creatives(self, last_id=0):
        url = f"https://marketing-api.vivo.com.cn/openapi/v1/ad/creative/pageInfoByLastId?" \
              f"access_token={self.__access_token}" \
              f"&timestamp={int(time.time() * 1000)}" \
              f"&nonce={hashlib.md5(str(time.time() * random.randint(1, 1000)).encode()).hexdigest()}"
        request_body = {
            'lastId': last_id,
            'pageSize': 10
        }
        response = requests.post(url, json=request_body).json()
        if not response['data']:
            print(request_body)
            print(response)
            return None
        return response


def main():
    test_job = VIVOApi('com.ld.ring')
    creative_info = test_job.get_single_creative_info('2023-10-01', '2023-10-01', 48255775)
    import json
    print(json.dumps(creative_info))


if __name__ == '__main__':
    main()











