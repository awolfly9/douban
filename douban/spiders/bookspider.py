# #-*- coding: utf-8 -*-
#
# import random
# import re
# import sys
# import requests
# import utils
# import config
#
# from scrapy.http import HtmlResponse
# from scrapy.http import Request
# from scrapy.spiders import Rule
# from scrapy.spiders import Spider, CrawlSpider
# from scrapy.linkextractors.sgml import SgmlLinkExtractor as sle
# from scrapy.selector import Selector
# from bs4 import BeautifulSoup
# from sqlhelper import SqlHelper, create_table
# from scrapy.utils.project import get_project_settings
#
# reload(sys)
# sys.setdefaultencoding('utf-8')
#
#
# class DouBanBookSpider(CrawlSpider):
#     name = 'book'
#     handle_httpstatus_list = [403]
#
#     start_urls = [
#         'https://book.douban.com/subject/26929221/',
#         'https://book.douban.com/subject/26609078/',
#         'https://book.douban.com/subject/26698660/',
#     ]
#
#     rules = [
#         Rule(sle(allow = '/subject/(\d+)/'), follow = True, callback = 'parse_book'),
#     ]
#
#     allowed_domains = [
#         'book.douban.com',
#     ]
#
#     headers = {
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#         'Accept-Encoding': 'gzip, deflate, br',
#         'Accept-Language': 'en-US,en;q=0.5',
#         'Cache-Control': 'max-age=0',
#         'Connection': 'keep-alive',
#         'Host': 'book.douban.com',
#         'Upgrade-Insecure-Requests': '1',
#         'User-Agent': random.choice(get_project_settings().get('USER_AGENTS'))
#     }
#
#     def __init__(self, *a, **kw):
#         super(DouBanBookSpider, self).__init__(*a, **kw)
#         self.dir_book = 'log/%s' % self.name
#
#         utils.make_dir(self.dir_book)
#
#         self.sql = SqlHelper()
#         self.table_name = config.douban_book
#         self.init()
#
#     def init(self):
#         command = self.get_create_table_command()
#         create_table(self.sql.cursor, self.sql.database, command)
#
#     def start_requests(self):
#         for i, url in enumerate(self.start_urls):
#             yield Request(
#                     url = url,
#                     dont_filter = True,
#                     method = 'GET',
#                     headers = self.headers,
#                     callback = self.parse,
#                     errback = self.error_parse,
#             )
#
#     def _requests_to_follow(self, response):
#         if not isinstance(response, HtmlResponse):
#             return
#         seen = set()
#         for n, rule in enumerate(self._rules):
#             links = [lnk for lnk in rule.link_extractor.extract_links(response)
#                      if lnk not in seen]
#             if links and rule.process_links:
#                 links = rule.process_links(links)
#             for link in links:
#                 seen.add(link)
#                 self.headers['User-Agent'] = random.choice(get_project_settings().get('USER_AGENTS'))
#
#                 # id = self.get_id(link.url)
#                 # if self.exist_id(id):
#                 #     self.log('requests_to_follow exist id:%s' % id)
#                 #     continue
#
#                 r = Request(
#                         url = link.url,
#                         headers = self.headers,
#                         meta = {
#                             'download_timeout': 10,
#                         },
#                         callback = self._response_downloaded,
#                         errback = self.error_parse,
#                 )
#                 r.meta.update(rule = n, link_text = link.text)
#                 yield rule.process_request(r)
#
#     def parse_book(self, response):
#         self.log('parse_book url:%s' % response.url)
#         url = response.url
#
#         id = self.get_id(url)
#
#         file_name = '%s/%s.html' % (self.dir_book, id)
#
#         self.save_page(file_name, response.body)
#         self.save_url(url)
#
#         text = response.body
#
#         soup = BeautifulSoup(text, 'lxml')
#         sel = Selector(text = text)
#
#         title = sel.xpath('//head/title/text()').extract_first()
#         self.log('title:%s' % title)
#
#         average = sel.xpath('//strong[@property="v:average"]/text()').extract_first()
#         self.log('average:%s' % average)
#
#         rating_people = sel.xpath('//a[@class="rating_people"]/span/text()').extract_first()
#         self.log('rating_people:%s' % rating_people)
#
#         rating_five = sel.xpath('//div[@class="rating_wrap clearbox"]/span[2]/text()').extract_first()
#         self.log('rating_five:%s' % rating_five)
#
#         rating_four = sel.xpath('//div[@class="rating_wrap clearbox"]/span[4]/text()').extract_first()
#         self.log('rating_four:%s' % rating_four)
#
#         info_author = sel.xpath('//div[@id="info"]/span/a/text()').extract_first()
#         self.log('info_author:%s' % info_author)
#
#         pattern = re.compile(r'<span class="pl">出版社:</span>(.*?)<br/>', re.S)
#         info_press = re.findall(pattern = pattern, string = text)
#         if len(info_press) > 0:
#             info_press = info_press[0]
#         else:
#             info_press = None
#         self.log('info_press:%s' % info_press)
#
#         pattern = re.compile(r'<span class="pl">出版年:</span>(.*?)<br/>', re.S)
#         info_release_date = re.findall(pattern = pattern, string = text)
#         if len(info_release_date) > 0:
#             info_release_date = info_release_date[0]
#         else:
#             info_release_date = None
#         self.log('info_release_date:%s' % info_release_date)
#
#         pattern = re.compile(r'<span class="pl">页数:</span>(.*?)<br/>', re.S)
#         info_page = re.findall(pattern = pattern, string = text)
#         if len(info_page) > 0:
#             info_page = info_page[0]
#         else:
#             info_page = None
#         self.log('info_page:%s' % info_page)
#
#         pattern = re.compile(r'<span class="pl">定价:</span>(.*?)<br/>', re.S)
#         info_price = re.findall(pattern = pattern, string = text)
#         if len(info_price) > 0:
#             info_price = info_price[0]
#         else:
#             info_price = None
#         self.log('info_price:%s' % info_price)
#
#         pattern = re.compile(r'<span class="pl">装帧:</span>(.*?)<br/>', re.S)
#         info_binding = re.findall(pattern = pattern, string = text)
#         if len(info_binding) > 0:
#             info_binding = info_binding[0]
#         else:
#             info_binding = None
#         self.log('info_binding:%s' % info_binding)
#
#         info_book_describe = soup.find(name = 'div', attrs = {'class': 'intro'})
#         if info_book_describe != None:
#             self.log('info_book_describe:%s' % info_book_describe.text)
#             info_book_describe = info_book_describe.text
#         else:
#             info_book_describe = None
#
#         info_author_describe = soup.find_all(name = 'div', attrs = {'class': 'intro'})
#         if len(info_author_describe) >= 1:
#             info_author_describe = info_author_describe[1].text
#             self.log('info_author_describe:%s' % info_author_describe)
#         else:
#             info_author_describe = None
#
#         msg = (
#             id, title, average, rating_people, rating_five, rating_four, info_author, info_press, info_page, info_price,
#             info_binding, info_release_date, info_book_describe, info_author_describe, url, None)
#
#         command = self.get_insert_data_command()
#
#         self.sql.insert_data(command, msg)
#
#     def error_parse(self, failure):
#         request = failure.request
#         self.log('error_request proxy:%s' % request.meta.get('proxy'))
#         proxy = request.meta.get('proxy')
#         rets = proxy.split(':')
#         ip = rets[1]
#         ip = ip[2:]
#         requests.get('http://127.0.0.1:8000/delete?name=%s&ip=%s' % ('douban', ip))
#
#     def get_id(self, url):
#         pattern = re.compile('/subject/(\d+)/', re.S)
#         id_group = re.search(pattern, url)
#         if id_group:
#             id = id_group.group(1)
#             return id
#
#         return None
#
#     def exist_id(self, id):
#         command = 'select id from {0} where id={1} limit 1;'.format(self.table_name, id)
#         data = self.sql.query_one(command)
#         if data != None:
#             return True
#
#         return False
#
#     def save_page(self, file_name, data):
#         with open(file_name, 'w') as f:
#             f.write(data)
#             f.close()
#
#     def save_url(self, data):
#         data += '\n'
#         with open('log/movies.txt', 'a') as f:
#             f.write(data)
#             f.close()
#
#     def get_create_table_command(self):
#         command = (
#             "CREATE TABLE IF NOT EXISTS {} ("
#             "`id` INT(9) NOT NULL AUTO_INCREMENT UNIQUE ,"
#             "`title` TEXT NOT NULL,"
#             "`average` CHAR(5) DEFAULT NULL,"
#             "`rating_people` INT(7) DEFAULT NULL,"
#             "`rating_five` CHAR(6) DEFAULT NULL,"
#             "`rating_four` CHAR(6) DEFAULT NULL ,"
#             "`info_author` CHAR(20) DEFAULT NULL,"
#             "`info_press` CHAR(20) DEFAULT NULL,"
#             "`info_page` CHAR(20) DEFAULT NULL,"
#             "`info_price` CHAR(20) DEFAULT NULL,"
#             "`info_binding` CHAR(20) DEFAULT NULL,"
#             "`info_release_date` CHAR(20) DEFAULT NULL,"
#             "`info_book_describe` TEXT DEFAULT NULL,"
#             "`info_author_describe` TEXT DEFAULT NULL,"
#             "`url` TEXT NOT NULL,"
#             "`save_time` TIMESTAMP NOT NULL,"
#             "PRIMARY KEY(id)"
#             ") ENGINE=InnoDB".format(self.table_name))
#         return command
#
#     def get_insert_data_command(self):
#         command = ("INSERT IGNORE INTO {} "
#                    "(id, title, average, rating_people, rating_five, rating_four, info_author, info_press, info_page, "
#                    "info_price, info_binding, info_release_date, info_book_describe, info_author_describe, url, "
#                    "save_time)"
#                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(self.table_name))
#
#         return command
