import scrapy
from NYtimes.items import NYtimesItem

class NytimesSpider(scrapy.Spider):
    name = "NYtimes"
    allowed_domains = ["nytimes.com"]
    # start_urls = []
    # for x in xrange(1860, 1865)
    # start_urls.append("http://spiderbites.nytimes.com/free_" + str(x) + "/index.html")

    start_urls = ["http://spiderbites.nytimes.com/free_2014/index.html"]
    baseURL = "http://spiderbites.nytimes.com"

    def parse(self, response):
        for url in response.xpath('//div[@class="articlesMonth"]/ul/li/a/@href').extract():
            yield scrapy.Request(self.baseURL + url, callback = self.parseNews)


    def parseNews(self, response):
        News = []
        News_urls = response.xpath('//ul[@id="headlines"]/li/a/@href').extract()
        News.extend([self.make_requests_from_url(url).replace(callback=self.parseSave) for url in News_urls])

    def parseSave(self, response):
        item = NYtimesItem();

        item["link"] = unicode(response.url)
        item["link"] = unicode(response.url.split("/")[-2])
        item["title"] = unicode(response.xpath('//meta[@name="hdl"]/@content').extract()[0])
        item["author"] = unicode(response.xpath('//meta[@name="byl"]/@content').extract()[0])
        item["date"] = unicode(response.xpath('//meta[@name="dat"]/@content').extract()[0])
        item["article"] = response.xpath('//p/text()').extract()
        yield item
