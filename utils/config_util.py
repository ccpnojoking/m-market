import os
from configparser import ConfigParser, ExtendedInterpolation

DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DIR, '../configs/configs.ini')
CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
CONFIG.read(CONFIG_PATH)


def main():
    init_config = CONFIG['init']['key']
    print(init_config)


if __name__ == '__main__':
    main()
