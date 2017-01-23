# coding=utf-8

import base64
import random
import requests
import json
import logging


class ProxyMiddleware(object):
    def process_request(self, request, spider):
        try:
            r = requests.get(url = 'http://127.0.0.1:8000/?name=douban', timeout = 10)
            data = json.loads(r.text)
            if len(data) > 0:
                proxy = random.choice(data)
                ip = proxy.get('ip')
                port = proxy.get('port')
                address = '%s:%s' % (ip, port)

                request.meta['proxy'] = 'http://%s' % address
                # request.meta['proxy'] = 'http://123.166.32.168:8118'
                logging.info('********ProxyMiddleware proxy*******:%s' % request.meta['proxy'])
        except Exception, e:
            logging.warning('ProxyMiddleware Exception:%s' % str(e))
