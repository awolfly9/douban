#-*- coding: utf-8 -*-

import random
import re
import sys
import logging
import requests
import utils
import config

from scrapy.http import HtmlResponse
from scrapy.http import Request
from scrapy.spiders import Rule
from scrapy.spiders import Spider, CrawlSpider
from scrapy.linkextractors.sgml import SgmlLinkExtractor as sle
from scrapy.selector import Selector
from bs4 import BeautifulSoup
from sqlhelper import SqlHelper, create_table
from scrapy.utils.project import get_project_settings

reload(sys)
sys.setdefaultencoding('utf-8')


class Movie(Spider):
    name = 'movie'

    def __init__(self, *a, **kw):
        super(Movie, self).__init__(*a, **kw)
        self.log_dir = 'log/%s' % self.name

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

        utils.make_dir(self.log_dir)
        self.init()

    def init(self):
        command = self.get_create_table_command()
        self.sql.execute(command)

    def start_requests(self):
        command = "SELECT * FROM {} WHERE crawled = \'no\'".format(config.douban_movie_urls)
        data = self.sql.query(command)
        for i, item in enumerate(data):
            yield Request(
                    url = item[1],
                    dont_filter = True,
                    method = 'GET',
                    headers = self.headers,
                    meta = {
                        'download_timeout': 20,
                    },
                    callback = self.parse_movie,
                    errback = self.error_parse,
            )

    def parse_movie(self, response):
        self.log('parse_movie url:%s' % response.url)
        url = response.url

        id = self.get_id(url)

        file_name = '%s/%s.html' % (self.log_dir, id)

        # self.save_page(file_name, response.body)
        # self.save_url(url)

        text = response.body

        soup = BeautifulSoup(text, 'lxml')
        sel = Selector(text = text)

        title = sel.xpath('//head/title/text()').extract_first()
        average = sel.xpath('//strong[@class="ll rating_num"]/text()').extract_first()
        rating_people = sel.xpath('//a[@class="rating_people"]/span/text()').extract_first()
        rating_five = sel.xpath('//div[@class="rating_wrap clearbox"]/span[2]/text()').extract_first()
        rating_four = sel.xpath('//div[@class="rating_wrap clearbox"]/span[4]/text()').extract_first()
        info_director = sel.xpath('//div[@id="info"]/span/span/a/text()').extract_first()
        info_screenwriter = sel.xpath('//div[@id="info"]/span[2]/span/a/text()').extract_first()
        info_starred = sel.xpath('//div[@id="info"]/span[3]/span/a/text()').extract_first()

        info_type = soup.find_all(name = 'span', attrs = {'property': 'v:genre'})
        types = ''
        for i, type in enumerate(info_type):
            if i != len(info_type) - 1:
                types = types + type.text + ' / '
            else:
                types = types + type.text
        info_type = types

        pattern = re.compile(r'<span class="pl">制片国家/地区:</span>(.*?)<br/>', re.S)
        info_region = re.findall(pattern = pattern, string = text)
        if len(info_region) > 0:
            info_region = info_region[0]
        else:
            info_region = None

        pattern = re.compile(r'<span class="pl">语言:</span>(.*?)<br/>', re.S)
        info_language = re.findall(pattern = pattern, string = text)
        if len(info_language) > 0:
            info_language = info_language[0]
        else:
            info_language = None

        soup.find(name = 'span', attrs = {'property': 'v:initialReleaseDate'})

        info_release_date = sel.xpath('//span[@property="v:initialReleaseDate"]/text()').extract_first()
        info_runtime = sel.xpath('//span[@property="v:runtime"]/text()').extract_first()

        pattern = re.compile(r'<span class="pl">又名:</span>(.*?)<br/>', re.S)
        info_other_name = re.findall(pattern = pattern, string = text)
        if len(info_other_name) > 0:
            info_other_name = info_other_name[0]
        else:
            info_other_name = None

        info_describe = sel.xpath('//span[@property="v:summary"]/text()').extract_first()

        msg = (id, title, average, rating_people, rating_five, rating_four, info_director, info_screenwriter,
               info_starred, info_type, info_region, info_language, info_release_date, info_runtime,
               info_other_name, info_describe, url, None)

        command = self.get_insert_data_command()

        self.sql.insert_data(command, msg)

        command = "UPDATE {0} SET crawled = \'{1}\' WHERE id = \'{2}\'".format(config.douban_movie_urls, 'yes', id)
        self.sql.execute(command)

    def error_parse(self, failure):
        request = failure.request
        self.log('error_parse url:%s meta:%s' % (request.url, str(request.meta)), logging.ERROR)

    def get_id(self, url):
        pattern = re.compile('/subject/(\d+)/', re.S)
        id_group = re.search(pattern, url)
        if id_group:
            id = id_group.group(1)
            return id

        return None

    def write_file(self, file_name, data):
        with open(file_name, 'w') as f:
            f.write(data)
            f.close()

    def get_query_command(self, id):
        command = "SELECT id from {0} WHERE id=\'{1}\'".format(config.douban_movie, id)
        return command

    def get_create_table_command(self):
        command = (
            "CREATE TABLE IF NOT EXISTS {} ("
            "`id` INT(8) NOT NULL AUTO_INCREMENT UNIQUE ,"
            "`title` TEXT NOT NULL,"
            "`average` FLOAT NOT NULL,"
            "`rating_people` INT(7) DEFAULT NULL,"
            "`rating_five` CHAR(5) DEFAULT NULL,"
            "`rating_four` CHAR(5) DEFAULT NULL ,"
            "`info_director` CHAR(20) DEFAULT NULL,"
            "`info_screenwriter` CHAR(20) DEFAULT NULL,"
            "`info_starred` CHAR(20) DEFAULT NULL,"
            "`info_type` CHAR(20) DEFAULT NULL,"
            "`info_region` CHAR(20) DEFAULT NULL,"
            "`info_language` CHAR(20) DEFAULT NULL,"
            "`info_release_date` CHAR(40) DEFAULT NULL,"
            "`info_runtime` CHAR(20) DEFAULT NULL,"
            "`info_other_name` TEXT DEFAULT NULL,"
            "`info_describe` TEXT DEFAULT NULL,"
            "`url` TEXT NOT NULL,"
            "`save_time` TIMESTAMP NOT NULL,"
            "PRIMARY KEY(id)"
            ") ENGINE=InnoDB".format(config.douban_movie))
        return command

    def get_insert_data_command(self):
        command = ("INSERT IGNORE INTO {} "
                   "(id, title, average, rating_people, rating_five, rating_four, info_director, info_screenwriter, "
                   "info_starred, info_type, info_region, info_language, info_release_date, info_runtime, "
                   "info_other_name, info_describe, url, save_time)"
                   "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                config.douban_movie))

        return command
