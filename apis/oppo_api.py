import requests
import os
import sys
import json

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR + '/../utils')

from utils.config_util import CONFIG
from utils.logger_util import Logger


class OPPOApi:

    def __init__(self, app, logger=None):
        self.__url = CONFIG['oppo'][app]
        self.logger = logger if logger else Logger('OPPO Api', os.path.join(DIR, 'logs/oppo_api.log'))

    def get_single_creative_info(self, query_date, creative_id):
        """
        :param query_date: '2023-11-03'
        """
        url = self.__url + f'?date_str={query_date}&adid={creative_id}'
        response = requests.get(url).json()
        if not response['result']:
            self.logger.error(response)
            return {}
        return json.loads(response['result'])


def main():
    test_job = OPPOApi('com.ld.ring')
    creative_info = test_job.get_single_creative_info('2023-10-02', 457011321)
    import json
    print(json.dumps(creative_info))


if __name__ == '__main__':
    main()











