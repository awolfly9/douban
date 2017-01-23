#-*- coding: utf-8 -*-


import sys

import logging
from scrapy import cmdline

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')

    logging.basicConfig(
            filename = 'log/spider.log',
            filemode = 'a',
            format = 'Spider:%(levelname)s:%(name)s:%(message)s',
            level = logging.DEBUG,
    )

    cmdline.execute('scrapy crawl movie'.split())
    # cmdline.execute('scrapy crawl book'.split())

