#!/usr/bin/python  
# -*- coding:utf-8 -*- 
import time, threading
import json
from lxml import etree
import requests
import sys
#from kafka import KafkaProducer


def parse_item(response):
    items = []


    platform = 'PC'
    songname = 'none'
    artist =  'none'
    listentime = 'none'
    username = 'none'
    for p in response.xpath('//*[@id="p-nowrap"]/div[1]/div[2]/div[1]/div/div[2]/div[2]/table/tbody/tr'):
        item = []
        try:
            platform = p.xpath('./td[3]/span/a/text()')[0]
        except IndexError:
            platform = 'PC'
        try:
            songname = p.xpath('./td[2]/a[1]/text()')[0]
        except IndexError:
            songname = 'none'
        try:
            artist = p.xpath('./td[2]/a[2]/text()')[0]
        except IndexError:
            artist = 'none'
        try:
            listentime = p.xpath('./td[4]/text()')[0]
        except IndexError:
            listentime = 'none'
        try:
            username = response.xpath('//*[@id="p-nowrap"]/div[1]/div[1]/div[1]/div/h1/text()')[0].split('最近在听的歌曲')[0]
        except IndexError:
            username = 'none'
        item.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        item.append(songname)
        item.append(artist)
        item.append(username)
        item.append(platform)
        item.append(listentime)
        items.append('    '.join(item))
    return items

def requestXiaMi(url):
    try:
        page = requests.get(url, headers=headers)
        response = etree.HTML(page.text)
        result = parse_item(response)

        if (url.find("/page/") < 0):#添加下一页
            page = len(response.xpath('//*[@id="column695"]/div[2]/div[1]/div/div[2]/div[4]/a'))
            for i in range(2, page-1):
                if (i > 4):
                    break;
                else:
                    result += requestXiaMi(url + "/page/" + str(i))
        return result
    except BaseException:
        print("error--------url:" + url)
        return []




url1 = 'http://www.xiami.com/space/charts-recent/u/3069638'
headers = {
    'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
    'Accept-Encoding':'gzip, deflate',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
}

#根据用户id获取该用户的抓取信息
def getUser(id):
    global userRecored
    return userRecored.get(id)

#获取初始化的用户对象Map
def getInitUser():
    userRecored1 = {}
    print("get user id from %s" % sys.argv[1])
    with open(sys.argv[1], encoding="utf-8") as f:
        user_origins = f.readlines()
        for item in user_origins:
            o = json.loads(item)
            id = o['id']
            userRecored1[id] = UserListen(id, 0, 0, 0)
    return userRecored1

#判断是否抓取指定用户的数据
def isScratchUser(id):
    user = getUser(id)
    if user.s_count == 0 :
        return False
    return True

#按照抓取的优先级进行排序
def sortUser(m):
    return m


#存储结果
def store(result, producer):
    # producer.send('music', b"1msg%d" % i, partition=0)
    # if  len(result) > 0:
        print(result)

class UserListen:
    def __init__(self, id, s_count, l_time, c_count):
        self.id = id #用户id
        self.s_count = s_count #最近抓取的数量
        self.l_time = l_time #最近抓取的时间，long
        self.c_count = c_count #抓取的次数

    def __setitem__(self, k, v):
        self.k = v

    # def __str__(self):
    #     return "id:%s, s_count:%d, l_time:%d, c_count:%d" % (self.id, self.s_count, self.l_time, self.c_count)


lock = threading.Lock()
ids = []
#多线程获取用户ID
def getUserId():
    lock.acquire()
    try:
        return ids.pop();
    except IndexError:
        return None
    finally:
        lock.release()

#线程执行完成的个数
finishCount = 0

#标记执行完成
def tabFinished():
    global finishCount
    lock.acquire()
    try:
        finishCount += 1
    finally:
        lock.release()


def scratchUser():
    # producer = KafkaProducer(bootstrap_servers=['192.168.1.63:9092','192.168.1.63:9093','192.168.1.63:9094'])
    count = 0
    print('thread %s is running...' % threading.current_thread().name)
    id = getUserId()
    while id != None :
        result = requestXiaMi('http://www.xiami.com/space/charts-recent/u/%s'% id)
        store(result, None)
        id = getUserId()
        count += 1
    print('thread %s ended. total id is %d' % threading.current_thread().name, count)
    tabFinished()


#多线程抓取
def multiThreadScratch(threadCount):
    for i in range(0, threadCount):
        t = threading.Thread(target=scratchUser, name='scratch--%d' % i)
        t.start()

    while True:
        if finishCount == threadCount:
            break
        time.sleep(2)

########################################### 开始 #####################################
#用户ID
userRecored = {}

userRecored = getInitUser()
ids = list(userRecored.keys())
multiThreadScratch(5)
