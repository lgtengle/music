#!/usr/bin/python  
# -*- coding:utf-8 -*- 
from scrapy import Spider 
from scrapy.selector import HtmlXPathSelector
from music.items import XiamiItem
from scrapy import Request
import time

cnt = 4
class DmozSpider(Spider):
    name = "xiami_listen"
    allowed_domains = ["www.xiami.com"]
    #设置爬取速度
    #download_delay = 1
    start_urls = [
        "http://www.xiami.com/space/charts-recent/u/3069638"
    ]
    #cnt = 10
    def parse(self, response):
        try:
            sites = response.xpath('//*[@id="p-nowrap"]/div[1]')
            url = response.url
            if  url.find("charts-recent")>-1: #解析音乐列表
                pages = sites.xpath('./div[2]/div[1]/div/div[2]/div[4]/span/text()').extract()
                self.parse_item(response)
                if  url.find("/page/")==-1:
                    if  len(pages)>0:
                        totalPage = int(int(pages[0][7:-2])/50)
                        for num in range(2, int(self.get_page(totalPage))):
                            yield Request(url + "/page/" + str(num), callback=self.parse_item)
                             
                    user_num = url[url.rindex("/") + 1:]
                    #返回用户关注链接      
                    yield Request("http://www.xiami.com/space/following/u/" + user_num)
                    #返回用户粉丝链接
                    yield Request("http://www.xiami.com/space/fans/u/" + user_num)
            else :
                for p in response.xpath('//*[@id="p-nowrap"]/div[1]/div[2]/div[1]/div/div[3]/div[1]/ul/li'):
                    yield Request("http://www.xiami.com/space/charts-recent" + p.xpath('./div[2]/p[1]/a/@href').extract()[0])
                pages_user = sites.xpath('./div[2]/div[1]/div/div[2]/div[4]/span/text()').extract()
                if  url.find("/page/")==-1 and len(pages_user)>0:
                    totalPage = int(pages_user.extract()[0][7:-2])/12
                    for num in range(2, self.get_page(totalPage)):
                        print(url + "/page/" + str(num))
                        yield Request(url + "/page/" + str(num))
        except BaseException:
            print("error--------url:"+response.url)
            
            
            
    def get_page(self, totalPage):
        count = 0
        if  totalPage > 4:
            count = 4
        else :
            count = totalPage
        return count + 1
    
    #解析用户
    def parse_user(self, response):
        #print('----------------------------------'+"http://www.xiami.com/space/charts-recent" )
        for p in response.xpath('//*[@id="p-nowrap"]/div[1]/div[2]/div[1]/div/div[3]/div[1]/ul/li'):
            yield Request("http://www.xiami.com/space/charts-recent" + p.xpath('./div[2]/p[1]/a/@href').extract())
    
    #解析音乐内容        
    def parse_item(self, response):
        #page = response.decode("utf-8")
        items = []
            
        platform = 'PC'
        songname = 'none'
        artist =  'none'
        listentime = 'none'
        username = 'none'
        for p in response.xpath('//*[@id="p-nowrap"]/div[1]/div[2]/div[1]/div/div[2]/div[2]/table/tbody/tr'):
            item = XiamiItem()
            try:
                platform = p.xpath('./td[3]/span/a/text()').extract()[0]
            except IndexError:
                platform = 'PC'
            try:
                songname = p.xpath('./td[2]/a[1]/text()').extract()[0]
            except IndexError:
                songname = 'none'
            try:
                artist = p.xpath('./td[2]/a[2]/text()').extract()[0]
            except IndexError:
                artist = 'none'
            try:
                listentime = p.xpath('./td[4]/text()').extract()[0]
            except IndexError:
                listentime = 'none'
            try:
                username = response.xpath('//*[@id="p-nowrap"]/div[1]/div[1]/div[1]/div/h1/text()').extract()[0].split('最近在听的歌曲')[0]
            except IndexError:
                username = 'none'
            '''  
            item = [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    songname,
                    artist,
                    username,
                    platform,
                    time]
            '''
    
            item['scratch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
            item['song'] = songname
            item['artist'] = artist
            item['user'] = username
            item['platform'] = platform
            item['time'] = listentime
            items.append(item) 
        return items
        