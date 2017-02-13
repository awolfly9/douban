#-*- coding: utf-8 -*-
import os
import sys

import logging
from scrapy import cmdline

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')

    if not os.path.exists('log'):
        os.makedirs('log')

    logging.basicConfig(
            filename = 'log/spider.log',
            format = '%(levelname)s %(asctime)s: %(message)s',
            level = logging.DEBUG
    )

    # cmdline.execute('scrapy crawl movie'.split())
    cmdline.execute('scrapy crawl book'.split())

