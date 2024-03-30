from scrapy import Spider, Request
import os
import datetime
import json
import sqlite3

SPLITTER = '!!TIME!!'

class ScrapyCache:

    caches = []
    conn = None


    def process_request(self, request: Request, spider: Spider):
        
        self.cache_enable = spider.custom_settings.get('cache_enable', False)
        self.cache_location = spider.custom_settings.get('cache_location', 'cache.db')
        self.cache_lifetime = spider.custom_settings.get('cache_lifetime', 60)
        self.spider = spider

        if self.conn == None:
            self.conn = sqlite3.connect(self.cache_location)
            self.c = self.conn.cursor()

            self.c.execute('''CREATE TABLE IF NOT EXISTS cache
             (url TEXT, time TEXT, content TEXT)''')

        force_refresh = request.meta.get('force_refresh', False) or request.meta.get('_force_refresh', False)

        if self.cache_enable and not force_refresh:
            
            url = request.url
            cache = self.find_cache(url)

            if cache and ((datetime.datetime.now() - cache['time']).seconds < self.cache_lifetime or self.cache_lifetime == -1):
                spider.logger.debug(f"cache used for <GET {url}>")
                return request.replace(url=f'file:///{self.get_dummy_path()}', meta={**request.meta, '_force_refresh': True, '_original_url': url, '_data': cache}, dont_filter=True)
            
            
    def process_response(self, request, response, spider):

        if response.status == 200:

            if not request.url.startswith('file:///'):

                self.update_data(request.url, response.text)

            else:
                response = response.replace(url=request.meta['_original_url'])
                response = response.replace(body=request.meta['_data']['content'].encode('utf8'))
                request.meta['_original_url'] = None

        request.meta['_force_refresh'] = False

        return response.replace()

    def find_cache(self, url):

        return self.form_data(self.c.execute("SELECT * FROM cache WHERE url=?", (url,)).fetchone())

    def get_dummy_path(self):
        return os.path.abspath(__file__)
    
    def form_data(self, data):
        if data == None:
            return data

        return {
            "url": data[0],
            "time": datetime.datetime.strptime(data[1], "%Y-%m-%d %H-%M-%S"),
            "content": data[2]
        }
    
    def update_data(self, url, content):

        self.c.execute("DELETE FROM cache WHERE url=?", (url,))

        now = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        self.c.execute("INSERT INTO cache (url, time, content) VALUES (?, ?, ?)", (url, now, content))

        self.conn.commit()


