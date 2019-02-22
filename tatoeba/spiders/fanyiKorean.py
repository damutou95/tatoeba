import scrapy
import re
import pymysql
from scrapy import Request
from urllib import parse
from tatoeba.items import TatoebaItem


class FanyiSpider(scrapy.Spider):
    name = 'fanyiKorean'
    #allowed_domains = ['sss']
    db = pymysql.Connect(host='127.0.0.1', user='root', password='18351962092', db='frequentwords')
    cursor = db.cursor()
    sql = 'select * from zh_full limit 0,50000'
    cursor.execute(sql)
    results = cursor.fetchall()
    start_urls = [f'https://tatoeba.org/eng/sentences/search?from=cmn&to=kor&query={parse.quote(row[0])}' for row in results]
    headers = {
        'accept':  'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        #'accept-encoding':  'gzip, deflate, br',
        'accept-language':  'zh-CN,zh;q=0.9',
        'cache-control':  'max-age=0',
        'cookie':  'CakeCookie=%7B%22interfaceLanguage%22%3A%22eng%22%7D; _ga=GA1.2.97595289.1550655280; CAKEPHP=cp1omj6ji7cep2g4srn2ahie23; csrfToken=14625be2280f85b4672fd43a8faa0703980990127c96d9a635fbaf7fb8d8d7bfdfdce68795c65fe1b89f2bf2a09ab7468041328ab7715bcc9dc60f69ab1e27de; _gid=GA1.2.1119544150.1550799914',
        #'referer':  'https://tatoeba.org/eng/sentences/search?query=%E6%88%91&from=und&to=und',
        #'upgrade-insecure-requests':  '1',
        'user-agent':  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        }

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse, headers=self.headers, dont_filter=True, meta={'tag': 0})

    def parse(self, response):
        try:
            sentenceCount = int(re.findall('\((\d+).*?result', response.text)[0])
        except IndexError:
            # 没有这个词的句子就把页数当作零
            sentenceCount = 0
        pageNum = sentenceCount // 10 if sentenceCount % 10 == 0 else sentenceCount // 10 + 1
        for i in range(pageNum):
            url = response.url + f'&page={str(i + 1)}' if i != 0 else response.url
            host = '127.0.0.1'
            user = 'root'
            passwd = '18351962092'
            dbname = 'tatoebaUrl'
            tablename = 'url'
            db = pymysql.connect(host, user, passwd, dbname)
            cursor = db.cursor()
            sql = f"select * from {tablename}"
            cursor.execute(sql)
            results = cursor.fetchall()
            db.commit()
            cursor.close()
            db.close()
            urls = []
            for row in results:
                urls.append(row[0])
            # 如果链接内容已经爬取过，那么不爬
            if url not in urls:
                yield Request(url=url, callback=self.parsePlus, headers=self.headers, dont_filter=True, meta={'tag': 0})

    def parsePlus(self, response):
        selectors = response.xpath('//div[@class="sentence-and-translations"]')
        for selector in selectors:
            #list = selector.re('<div class=\"text\" flex\n(.*?)dir="ltr">\n\s+(.*?)\s+</div>')
            # with open('text.txt', 'a') as f:
            #     f.write(response.text)
            list = [x.strip('\n').strip() for x in selector.re('<div class="text" flex dir="ltr">([\s\S]*?)</div>')[1:]]
            for x in list:
                item = TatoebaItem()
                item['chinese'] = selector.xpath('string(.//div[@class="text"])').extract_first().strip('\n').strip()
                item['target'] = x
                item['language'] = 'korean'
                yield item



