import os
import sys
import requests

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR + '/../utils')

from utils.config_util import CONFIG


class MApi:

    def __init__(self, app):
        self.__payer_count_url = CONFIG['my_payer_count_url'][app]

    def get_payer_count(self, date):
        """
        :param date: '2023-10-25'
        """
        url = f"{self.__payer_count_url}{date}"
        response = requests.get(url).json()
        return response


def main():
    job = MApi('com.ld.ring')
    payer_count = job.get_payer_count('2023-10-25')
    print(payer_count)


if __name__ == '__main__':
    main()
