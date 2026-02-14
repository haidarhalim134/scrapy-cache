from scrapy import Spider, Request
from scrapy.exceptions import IgnoreRequest
from scrapy.http.response.html import HtmlResponse
import re
import os
import datetime
import json
import sqlite3
import time
import random
import scrapy
import scrapy.http
import scrapy.http.response
import scrapy.http.response.html

SPLITTER = '!!TIME!!'

class ScrapyCache:

    caches = []
    conn = None


    def process_request(self, request: Request, spider: Spider):
        
        self.cache_enable = spider.custom_settings.get('cache_enable', False)
        self.ignore_non_cached = spider.custom_settings.get('ignore_non_cached', False)
        self.cache_location = spider.custom_settings.get('cache_location', 'cache.db')
        self.cache_lifetime = spider.custom_settings.get('cache_lifetime', 60)
        self.non_cache_delay = spider.custom_settings.get('non_cache_delay', 0)
        self.spider = spider

        if self.conn == None:
            self.conn = CacheManager(self.cache_location)

        force_refresh = request.meta.get('force_refresh', False) or request.meta.get('_force_refresh', False)
        prefetch_check = request.meta.get('cache_prefetch_check', lambda url, cache: True)

        if self.cache_enable and not force_refresh:
            
            url = request.url
            if request.meta.get("cache_id", None):
                url = request.meta["cache_id"]
            cache = self.conn.find_cache(url)

            if cache and ((datetime.datetime.now() - cache['time']).seconds < self.cache_lifetime or self.cache_lifetime == -1) and prefetch_check(url, cache):

                ###
                # PATCH: scrapy driver patch
                ###
                # if "init_request" in request.meta:
                #     del request.meta['init_request']

                spider.logger.debug(f"cache used for <GET {url}>")
                request.meta['is_cache'] = True
                return HtmlResponse(
                    url=request.url,
                    body=cache['content'].encode('utf8'),
                    encoding="utf8",
                    request=request,
                    status=200
                )
        
        if self.ignore_non_cached:
            raise IgnoreRequest("non cached request ignored")

        delay = self.non_cache_delay() if callable(self.non_cache_delay) else self.non_cache_delay
        if self.cache_enable and delay > 0 and not request.url.startswith('file:///'):
            spider.crawler.engine.pause()
            time.sleep(delay) 
            spider.crawler.engine.unpause()

    req = 0  
    def process_response(self, request, response, spider):

        if response.status == 200:

            prestore_check = request.meta.get('cache_prestore_check', None)

            if not request.url.startswith('file:///'):
                
                url = request.url
                if request.meta.get("cache_id", None):
                    url = request.meta["cache_id"]

                if not request.meta.get('is_cache', False) and (not hasattr(spider, 'prestore_check') or spider.prestore_check(url, response)) and (not prestore_check or prestore_check(url, response)):
                    self.conn.update_data(url, response.text)
                else:
                    del request.meta['is_cache']

                # TODO: this potentially faulty as I wont have the cache id for the root url
                if "redirect_urls" in request.meta:
                    self.conn.update_data(request.meta['redirect_urls'][0], response.text)

            else:
                # response = response.replace(url=request.meta['_original_url'])
                response = response.replace(body=request.meta['_data']['content'].encode('utf8'))
                request.meta['_original_url'] = None

        request.meta['_force_refresh'] = False
        if "cache_id" in request.meta:
            del request.meta["cache_id"]

        return response.replace()

    def get_dummy_path(self):
        return os.path.abspath(__file__)


class CacheManager:

    def __init__(self, db_location):
        self.conn = sqlite3.connect(db_location)
        self.c = self.conn.cursor()

        self.c.execute('''CREATE TABLE IF NOT EXISTS cache
            (url TEXT, time TEXT, content TEXT)''')
        
    def update_data(self, url, content):

        self.c.execute("DELETE FROM cache WHERE url=?", (url,))

        now = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        self.c.execute("INSERT INTO cache (url, time, content) VALUES (?, ?, ?)", (url, now, content))

        self.conn.commit()

    def form_data(self, data):
        if data == None:
            return data

        return {
            "url": data[0],
            "time": datetime.datetime.strptime(data[1], "%Y-%m-%d %H-%M-%S"),
            "content": data[2]
        }

    def find_cache(self, url):

        return self.form_data(self.c.execute("SELECT * FROM cache WHERE url like ?", (url,)).fetchone())