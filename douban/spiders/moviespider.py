#-*- coding: utf-8 -*-
import random
import re
import sys

from scrapy.http import HtmlResponse
from scrapy.http import Request
from scrapy.spiders import Rule
from scrapy.spiders import Spider, CrawlSpider
from scrapy.linkextractors.sgml import SgmlLinkExtractor as sle
from scrapy.selector import Selector
from bs4 import BeautifulSoup
from utils import *
from config import *
from SqlHelper import SqlHelper
from scrapy.utils.project import get_project_settings

reload(sys)
sys.setdefaultencoding('utf-8')

class DouBanMovieSpider(CrawlSpider):
    name = 'movie'
    handle_httpstatus_list = [403]

    start_urls = [
        'https://movie.douban.com/subject/3434070/?from=subject-page',
        'https://movie.douban.com/subject/4746257/?from=subject-page',
        'https://movie.douban.com/subject/26390631/?from=subject-page',
    ]

    rules = [
        Rule(sle(allow = '/subject/(\d+)/\?from=subject-page'), follow = True, callback = 'parse_movie'),
    ]

    allowed_domains = [
        'movie.douban.com',
    ]

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Host': 'movie.douban.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': random.choice(get_project_settings().get('USER_AGENTS'))
    }

    def __init__(self, *a, **kw):
        super(DouBanMovieSpider, self).__init__(*a, **kw)
        self.dir_movie = 'movie'
        self.dir_log = 'log'

        self.sql = SqlHelper()
        self.table_name = douban_movie_table_name
        self.init()

    def init(self):
        command = get_create_table_command(self.table_name)
        self.sql.create_table(command)

    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield Request(
                    url = url,
                    dont_filter = True,
                    method = 'GET',
                    # meta = {
                    #     'cookiejar': i
                    # },
                    headers = self.headers,
                    callback = self.parse
            )

    def _requests_to_follow(self, response):
        if not isinstance(response, HtmlResponse):
            return
        seen = set()
        for n, rule in enumerate(self._rules):
            links = [lnk for lnk in rule.link_extractor.extract_links(response)
                     if lnk not in seen]
            if links and rule.process_links:
                links = rule.process_links(links)
            for link in links:
                seen.add(link)
                self.headers['User-Agent'] = random.choice(get_project_settings().get('USER_AGENTS'))

                id = self.get_id(link.url)
                if self.exist_id(id):
                    self.log('requests_to_follow exist id:%s' % id)
                    continue

                r = Request(
                        url = link.url,
                        # cookies = response.meta.get('cookiejar', ''),
                        headers = self.headers,
                        callback = self._response_downloaded
                )
                r.meta.update(rule = n, link_text = link.text)
                yield rule.process_request(r)

    def parse_movie(self, response):
        self.log('parse_movie url:%s' % response.url)
        url = response.url

        id = self.get_id(url)

        file_name = '%s/%s.html' % (self.dir_movie, id)

        self.save_page(file_name, response.body)
        self.save_url(url)

        text = response.body

        soup = BeautifulSoup(text, 'lxml')
        sel = Selector(text = text)

        title = sel.xpath('//head/title/text()').extract_first()
        self.log('title:%s' % title)

        average = sel.xpath('//strong[@class="ll rating_num"]/text()').extract_first()
        self.log('average:%s' % average)

        rating_people = sel.xpath('//a[@class="rating_people"]/span/text()').extract_first()
        self.log('rating_people:%s' % rating_people)

        rating_five = sel.xpath('//div[@class="rating_wrap clearbox"]/span[2]/text()').extract_first()
        self.log('rating_five:%s' % rating_five)

        rating_four = sel.xpath('//div[@class="rating_wrap clearbox"]/span[4]/text()').extract_first()
        self.log('rating_four:%s' % rating_four)

        info_director = sel.xpath('//div[@id="info"]/span/span/a/text()').extract_first()
        self.log('info_director:%s' % info_director)

        info_screenwriter = sel.xpath('//div[@id="info"]/span[2]/span/a/text()').extract_first()
        self.log('info_screenwriter:%s' % info_screenwriter)

        info_starred = sel.xpath('//div[@id="info"]/span[3]/span/a/text()').extract_first()
        self.log('info_starred:%s' % info_starred)

        info_type = soup.find_all(name = 'span', attrs = {'property': 'v:genre'})
        types = ''
        for i, type in enumerate(info_type):
            if i != len(info_type) - 1:
                types = types + type.text + ' / '
            else:
                types = types + type.text
        self.log('info_type:%s' % types)

        info_type = types

        pattern = re.compile(r'<span class="pl">制片国家/地区:</span>(.*?)<br/>', re.S)
        info_region = re.findall(pattern = pattern, string = text)
        if len(info_region) > 0:
            info_region = info_region[0]
        else:
            info_region = None
        self.log('info_region:%s' % info_region)

        pattern = re.compile(r'<span class="pl">语言:</span>(.*?)<br/>', re.S)
        info_language = re.findall(pattern = pattern, string = text)
        if len(info_language) > 0:
            info_language = info_language[0]
        else:
            info_language = None
        self.log('info_language:%s' % info_language)

        soup.find(name = 'span', attrs = {'property': 'v:initialReleaseDate'})

        info_release_date = sel.xpath('//span[@property="v:initialReleaseDate"]/text()').extract_first()
        self.log('info_release_date:%s' % info_release_date)

        info_runtime = sel.xpath('//span[@property="v:runtime"]/text()').extract_first()
        self.log('info_runtime:%s' % info_runtime)

        pattern = re.compile(r'<span class="pl">又名:</span>(.*?)<br/>', re.S)
        info_other_name = re.findall(pattern = pattern, string = text)
        if len(info_other_name) > 0:
            info_other_name = info_other_name[0]
        else:
            info_other_name = None
        self.log('info_other_name:%s' % info_other_name)

        info_describe = sel.xpath('//span[@property="v:summary"]/text()').extract_first()
        self.log('info_describe:%s' % info_describe)

        msg = (id, title, average, rating_people, rating_five, rating_four, info_director, info_screenwriter,
               info_starred, info_type, info_region, info_language, info_release_date, info_runtime,
               info_other_name, info_describe, url, None)

        command = get_insert_data_command(self.table_name)

        self.sql.insert_data(command, msg)

    def get_id(self, url):
        pattern = re.compile('/subject/(\d+)/', re.S)
        id_group = re.search(pattern, url)
        if id_group:
            id = id_group.group(1)
            return id

        return None

    def exist_id(self, id):
        command = 'select id from {0} where id={1} limit 1;'.format(self.table_name, id)
        data = self.sql.query_one(command)
        if data != None:
            return True

        return False

    def save_page(self, file_name, data):
        with open(file_name, 'w') as f:
            f.write(data)
            f.close()

    def save_url(self, data):
        data += '\n'
        with open('log/movies.txt', 'a') as f:
            f.write(data)
            f.close()
