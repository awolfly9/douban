#-*- coding: utf-8 -*-

import random
import re
import sys
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


class BookSpider(CrawlSpider):
    name = 'book'

    def __init__(self, *a, **kw):
        super(BookSpider, self).__init__(*a, **kw)
        self.dir_book = 'log/%s' % self.name

        self.sql = SqlHelper()
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': 'book.douban.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:51.0) Gecko/20100101 Firefox/51.0',
        }

        utils.make_dir(self.dir_book)
        self.init()

    def init(self):
        command = self.get_create_table_command()
        self.sql.execute(command)

    def start_requests(self):
        command = "SELECT * FROM {} WHERE crawled = \'no\'".format(config.douban_book_urls)
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
                    callback = self.parse_book,
                    errback = self.error_parse,
            )

    def parse_book(self, response):
        self.log('parse_book url:%s' % response.url)
        url = response.url

        id = self.get_id(url)

        file_name = '%s/%s.html' % (self.dir_book, id)

        self.write_file(file_name, response.body)

        text = response.body

        soup = BeautifulSoup(text, 'lxml')
        sel = Selector(text = text)

        title = sel.xpath('//head/title/text()').extract_first()
        average = sel.xpath('//strong[@property="v:average"]/text()').extract_first()
        rating_people = sel.xpath('//a[@class="rating_people"]/span/text()').extract_first()

        rating_five = sel.xpath('//div[@class="rating_wrap clearbox"]/span[2]/text()').extract_first()

        rating_four = sel.xpath('//div[@class="rating_wrap clearbox"]/span[4]/text()').extract_first()
        info_author = sel.xpath('//div[@id="info"]/span/a/text()').extract_first()

        pattern = re.compile(r'<span class="pl">出版社:</span>(.*?)<br/>', re.S)
        info_press = re.findall(pattern = pattern, string = text)
        if len(info_press) > 0:
            info_press = info_press[0]
        else:
            info_press = None

        pattern = re.compile(r'<span class="pl">出版年:</span>(.*?)<br/>', re.S)
        info_release_date = re.findall(pattern = pattern, string = text)
        if len(info_release_date) > 0:
            info_release_date = info_release_date[0]
        else:
            info_release_date = None

        pattern = re.compile(r'<span class="pl">页数:</span>(.*?)<br/>', re.S)
        info_page = re.findall(pattern = pattern, string = text)
        if len(info_page) > 0:
            info_page = info_page[0]
        else:
            info_page = None

        pattern = re.compile(r'<span class="pl">定价:</span>(.*?)<br/>', re.S)
        info_price = re.findall(pattern = pattern, string = text)
        if len(info_price) > 0:
            info_price = info_price[0]
        else:
            info_price = None

        pattern = re.compile(r'<span class="pl">装帧:</span>(.*?)<br/>', re.S)
        info_binding = re.findall(pattern = pattern, string = text)
        if len(info_binding) > 0:
            info_binding = info_binding[0]
        else:
            info_binding = None

        info_book_describe = soup.find(name = 'div', attrs = {'class': 'intro'})
        if info_book_describe != None:
            info_book_describe = info_book_describe.text
        else:
            info_book_describe = None

        info_author_describe = soup.find_all(name = 'div', attrs = {'class': 'intro'})
        if len(info_author_describe) > 1:
            info_author_describe = info_author_describe[1].text
        else:
            info_author_describe = None

        msg = (
            id, title, average, rating_people, rating_five, rating_four, info_author, info_press, info_page, info_price,
            info_binding, info_release_date, info_book_describe, info_author_describe, url, None)

        command = self.get_insert_data_command()

        self.sql.insert_data(command, msg)

        command = "UPDATE {0} SET crawled = \'{1}\' WHERE id = \'{2}\'".format(config.douban_book_urls, 'yes', id)
        self.sql.execute(command)

    def error_parse(self, failure):
        request = failure.request
        self.log('error_parse url:%s proxy:%s' % (request.url, str(request.meta)))

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
        command = "SELECT id from {0} WHERE id=\'{1}\'".format(config.douban_book, id)
        return command

    def get_create_table_command(self):
        command = (
            "CREATE TABLE IF NOT EXISTS {} ("
            "`id` INT(9) NOT NULL UNIQUE ,"
            "`title` TEXT NOT NULL,"
            "`average` CHAR(5) DEFAULT NULL,"
            "`rating_people` INT(7) DEFAULT NULL,"
            "`rating_five` CHAR(6) DEFAULT NULL,"
            "`rating_four` CHAR(6) DEFAULT NULL ,"
            "`info_author` CHAR(20) DEFAULT NULL,"
            "`info_press` CHAR(20) DEFAULT NULL,"
            "`info_page` CHAR(20) DEFAULT NULL,"
            "`info_price` CHAR(20) DEFAULT NULL,"
            "`info_binding` CHAR(20) DEFAULT NULL,"
            "`info_release_date` CHAR(20) DEFAULT NULL,"
            "`info_book_describe` TEXT DEFAULT NULL,"
            "`info_author_describe` TEXT DEFAULT NULL,"
            "`url` TEXT NOT NULL,"
            "`save_time` TIMESTAMP NOT NULL,"
            "PRIMARY KEY(id)"
            ") ENGINE=InnoDB".format(config.douban_book))
        return command

    def get_insert_data_command(self):
        command = ("INSERT IGNORE INTO {} "
                   "(id, title, average, rating_people, rating_five, rating_four, info_author, info_press, info_page, "
                   "info_price, info_binding, info_release_date, info_book_describe, info_author_describe, url, "
                   "save_time)"
                   "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(config.douban_book))

        return command
