import json
import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
import os
import sys

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR + '/../utils')

from logger_util import Logger


class DingTalk:
    def __init__(self, access_token=None, secret=None, logger=None):
        super().__init__()
        self.__access_token = access_token
        self.__secret = secret
        self.logger = logger if logger else Logger('Ding Talk Util', os.path.join(DIR, 'logs/ding_talk_util.log'))

    def _get_url(self):
        timestamp = str(round(time.time() * 1000))
        url = f'https://oapi.dingtalk.com/robot/send?' \
              f'access_token={self.__access_token}' \
              f'&timestamp={timestamp}'
        if not self.__secret:
            return url
        sign = self._get_sign(self.__secret)
        return url + f'&sign={sign}'

    @staticmethod
    def _get_sign(secret):
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return sign

    def alert(self, alert_info):
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        json_text = {
            "msgtype": "markdown",
            'markdown': {
                'title': 'alert',
                'text': alert_info
            }
        }
        url = self._get_url()
        try:
            response = requests.post(url, json.dumps(json_text), headers=headers).json()
            self.logger.info(response)
        except Exception as e:
            self.logger.error(e)
