import os
import sys
import datetime
import pandas as pd


def run():
    DIR = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(DIR, 'files/one_off/one_off.csv')

    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
        print('a')
        return
    a = {'a': ['1', '2']}
    a = pd.DataFrame(a)
    a.to_csv(path, index=False)


run()

