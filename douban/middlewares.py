# coding=utf-8

import base64
import random
import requests
import json
import logging
import utils

from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError

from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
from scrapy.xlib.tx import ResponseFailed
from scrapy.core.downloader.handlers.http11 import TunnelError

logger = logging.getLogger(__name__)


class ProxyMiddleware(object):
    def process_request(self, request, spider):
        try:
            request.meta['req_count'] = request.meta.get('req_count', 0) + 1

            if request.meta.get('is_proxy', True):
                request.meta['proxy'] = proxy_mng.get_proxy()
        except Exception, e:
            logging.warning('ProxyMiddleware Exception:%s' % str(e))

    def process_exception(self, request, exception, spider):
        logging.error(
                'process_exception error_request request exception:%s url:%s  proxy:%s' % (
                    exception, request.url, str(request.meta)))

        if request.meta.get('is_proxy', True):
            proxy_mng.delete_proxy(request.meta.get('proxy'))
            request.meta['proxy'] = proxy_mng.get_proxy()

        return request


class CustomRetryMiddleware(object):
    # IOError is raised by the HttpCompression middleware when trying to
    # decompress an empty response
    EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)

    def __init__(self, settings):
        if not settings.getbool('RETRY_ENABLED'):
            raise NotConfigured
        self.max_retry_times = settings.getint('RETRY_TIMES')
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            return self._retry(request, exception, spider)

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        if retries <= self.max_retry_times:
            logger.debug("Retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra = {'spider': spider})
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            retryreq.priority = request.priority + self.priority_adjust

            request.meta['req_count'] = request.meta.get('req_count', 0) + 1

            if retries == self.max_retry_times:
                if request.meta.get('is_proxy', True):
                    proxy_mng.delete_proxy(retryreq.meta.get('proxy'))
                    retryreq.meta['proxy'] = proxy_mng.get_proxy()

            return retryreq
        else:
            logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra = {'spider': spider})


class ProxyManager(object):
    def __init__(self):
        self.index = 0
        self.proxys = []

        self.update_proxy()

    def update_proxy(self):
        try:
            r = requests.get(url = 'http://127.0.0.1:8000/select?name=douban', timeout = 10)
            data = json.loads(r.text)
            for item in data:
                self.proxys.append(item)

            logger.debug('*****************proxy manager  proxys:%s****************' % (str(self.proxys)))
        except:
            pass

    def get_proxy(self):
        if len(self.proxys) <= 0:
            self.update_proxy()

        if len(self.proxys) > 0:
            self.index = self.index + 1
            self.index = self.index % len(self.proxys)

            proxy = 'http://%s:%s' % (self.proxys[self.index].get('ip'), self.proxys[self.index].get('port'))
            utils.log('++++++++++proxy:%s++++++++++' % proxy)
            return proxy

        return None

    def delete_proxy(self, proxy):
        try:
            rets = proxy.split(':')
            ip = rets[1]
            ip = ip[2:]

            for item in self.proxys:
                if item.get('ip') == ip:
                    self.proxys.remove(item)
                    break

            if len(self.proxys) < 3:
                self.update_proxy()

            logger.debug('--------------delete ip:%s-----------' % ip)
            r = requests.get('http://127.0.0.1:8000/delete?name=%s&ip=%s' % ('douban', ip))
            return r.text
        except:
            return False


proxy_mng = ProxyManager()
