from scrapy import Spider, Request
import os
import datetime
import json

SPLITTER = '!!TIME!!'


class ScrapyCache:

    caches = []

    def process_request(self, request: Request, spider: Spider):
        
        self.cache_enable = spider.custom_settings.get('cache_enable', False)
        self.cache_base_loc = spider.custom_settings.get('cache_location', './cache')
        self.cache_lifetime = spider.custom_settings.get('cache_lifetime', 60)
        self.spider = spider

        force_refresh = request.meta.get('force_refresh', False) or request.meta.get('_force_refresh', False)

        if self.cache_enable and not force_refresh:
            
            url = request.url
            cache = self.find_cache(url)

            if cache and (datetime.datetime.now() - cache[0]).seconds < self.cache_lifetime:
                spider.logger.debug(f"cache used for <GET {url}>")
                return request.replace(url=f'file:///{self.get_cache_path(cache)}', meta={**request.meta, '_force_refresh': True, '_original_url': url})
            
            
    def process_response(self, request, response, spider):

        if response.status == 200:
            
            existing_cache = self.find_cache(request.url)
            if existing_cache:
                os.remove(self.get_cache_path(existing_cache))

            if not request.url.startswith('file:///'):
                with open(self.get_cache_path(self.encode(request.url)), 'w+', encoding='utf8') as fl:
                    fl.write(response.text)
            else:
                response = response.replace(url=request.meta['_original_url'])
                request.meta['_original_url'] = None

        request.meta['_force_refresh'] = False

        return response.replace()

    def quote(self, url):
        return "".join([x if x.isalnum() or x in "._- " else "_" for x in url])

    def encode(self, url):
        return f'{datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")}{SPLITTER}{self.quote(url)}'

    def decode(self, string):
        string = string.split(SPLITTER)
        string[0] = datetime.datetime.strptime(string[0], "%Y-%m-%d %H-%M-%S")
        return string if len(string) == 2 else [string[0], SPLITTER.join(string[1:])]

    def find_cache(self, url):

        self.caches = [self.decode(x) for x in os.listdir(self.cache_base_loc)]

        res = [x for x in self.caches if x[1] == self.quote(url)]
        if res:
            return res[0]
        
    def get_cache_path(self, cache):
        if isinstance(cache, list):
            cache[0] = cache[0].strftime("%Y-%m-%d %H-%M-%S")
            return f'{os.path.abspath(self.cache_base_loc)}/{SPLITTER.join(cache)}'
        else:
            return f'{os.path.abspath(self.cache_base_loc)}/{cache}'



