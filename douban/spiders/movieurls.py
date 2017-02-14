#-*- coding: utf-8 -*-

import random
import logging
import utils
import config

from sqlhelper import SqlHelper
from scrapy.utils.response import get_base_url
from scrapy.utils.project import get_project_settings
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.selector import Selector


class Movieurls(Spider):
    name = 'movie_urls'

    start_urls = [
        'https://movie.douban.com/tag/'
    ]

    def __init__(self, *a, **kw):
        super(Movieurls, self).__init__(*a, **kw)
        self.log_dir = 'log/%s' % self.name

        utils.make_dir(self.log_dir)

        self.sql = SqlHelper()
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': 'movie.douban.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:51.0) Gecko/20100101 Firefox/51.0',
        }

        self.init()

    def init(self):
        command = (
            "CREATE TABLE IF NOT EXISTS {} ("
            "`id` INT(9) NOT NULL UNIQUE ,"
            "`url` TEXT NOT NULL,"
            "`tag` CHAR(20) NOT NULL,"
            "`crawled` CHAR(5) NOt NULl,"
            "PRIMARY KEY(id)"
            ") ENGINE=InnoDB".format(config.douban_movie_urls))

        self.sql.execute(command)

    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield Request(
                    url = url,
                    dont_filter = True,
                    method = 'GET',
                    headers = self.headers,
                    meta = {
                        'download_timeout': 20,
                        'is_proxy': False,
                    },
                    callback = self.get_all_category,
                    errback = self.error_parse,
            )

    def get_all_category(self, response):
        self.write_file('%s/category.html' % self.log_dir, response.body)
        tags = response.xpath('//table/tbody/tr/td/a/@href').extract()
        for tag in tags:
            res = tag.split('/')
            res = res[len(res) - 1]
            utils.log('tag:%s' % tag)

            url = response.urljoin(tag)
            yield Request(
                    url = url,
                    headers = self.headers,
                    dont_filter = True,
                    meta = {
                        'tag': res,
                        'download_timeout': 20,
                        # 'is_proxy': False,
                    },
                    callback = self.get_page,
                    errback = self.error_parse
            )

    def get_page(self, response):
        self.write_file('%s/%s.html' % (self.log_dir, 'page'), response.body)

        items = response.xpath('//div[@class=""]/table/tr/td[2]/div/a/@href').extract()
        for item in items:
            self.log('item:%s' % item)
            id = self.get_movie_id(item)

            command = "INSERT IGNORE INTO {}(id, url, tag, crawled) VALUES(%s, %s, %s, %s)".format(
                    config.douban_movie_urls)
            data = (id, item, response.meta.get('tag', ''), 'no')

            self.sql.insert_data(command, data)

        next_url = response.xpath('//span[@class="next"]/a/@href').extract_first()
        if next_url != None:
            url = response.urljoin(next_url)
            yield Request(
                    url = url,
                    headers = self.headers,
                    dont_filter = True,
                    meta = {
                        'tag': response.meta.get('tag'),
                        'download_timeout': 20,
                    },
                    callback = self.get_page,
                    errback = self.error_parse
            )

    def error_parse(self, failure):
        request = failure.request
        utils.log('error_parse url:%s meta:%s' % (request.url, str(request.meta)), logging.ERROR)
        pass

    def get_movie_id(self, item):
        res = item.split('/')
        id = res[len(res) - 2]
        return id

    def write_file(self, file_name, data):
        with open("%s" % file_name, 'w') as f:
            f.write(data)
            f.close()
