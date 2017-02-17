#-*- coding: utf-8 -*-

import requests
import json
import utils


class ProxyManager(object):
    def __init__(self):
        self.index = 0
        self.proxys = []

        self.update_proxy()

        self.address = 'http://127.0.0.1:8000'

    def update_proxy(self):
        try:
            r = requests.get(url = '%s/select?name=douban&anonymity=1' % self.address, timeout = 10)
            data = json.loads(r.text)
            for item in data:
                self.proxys.append(item)

            utils.log('*****************proxy manager  proxys:%s****************' % (str(self.proxys)))
        except:
            pass

    def get_proxy(self):
        if len(self.proxys) <= 10:
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

            utils.log('--------------delete ip:%s-----------' % ip)
            r = requests.get(url = '%s/delete?name=%s&ip=%s' % (self.address, 'douban', ip))
            return r.text
        except:
            return False


proxymng = ProxyManager()
