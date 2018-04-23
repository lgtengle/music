#!/usr/bin/python
# -*- coding:utf-8 -*-

from lxml import etree
import requests
import json


url1 = 'http://www.xiami.com/space/charts-recent/u/3069638'
headers = {
    'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
    'Accept-Encoding':'gzip, deflate',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
}


userRecored1 = []
with open('D:/data/music/user1.json', encoding="utf-8") as f:
    user_origins = f.readlines()
    for item in user_origins:
        o = json.loads(item)
        userRecored1.append(o['id'])

userIds = []
for id in userRecored1:
    try:
        print(id)
        page = requests.get('http://www.xiami.com/space/charts-recent/u/' + id, headers=headers)
        response = etree.HTML(page.text)
        length = len(response.xpath('//*[@id="column695"]/div[2]/div[1]/div/div[2]/div[2]/table/tbody/tr'))
        if length > 0:
            userIds.append(id)
    except BaseException:
        print("error--------id:" + id)

with open('D:/data/music/filter_user.txt', 'w') as f:
    f.write(','.join(userIds))

