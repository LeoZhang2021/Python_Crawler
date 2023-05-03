# Leo Zhang
# 2023 Apr

# encode = utf8

# important points
'''
Python
Crawler
requests and json library
MySQL and pymysql library
multi thread
'''

# html
import requests
import json
import random
import pymysql

# logic
import time
from multiprocessing.dummy import Pool as ThreadPool

class bilibili_crawler(object):
    '''
    crawl user info from bilibili.com
    '''

    def __init__(self):

        self.ua_list = self.LoadUA("user_agents.txt")
        self.ua_rand = random.choice(self.ua_list);
        self.head = {
            'User-Agent': self.ua_rand,
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://space.bilibili.com/45388',
            'Origin': 'http://space.bilibili.com',
            'Host': 'space.bilibili.com',
            'AlexaToolbar-ALX_NS_PH': 'AlexaToolbar/alx-4.0',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }

        # proxies found online ( will be blocked after a few try. need time to work again)
        self.proxies = {
            'http': 'http://191.231.62.142:8000',
            'http': '218.89.51.167',
            'http': '123.138.214.150',
            'https': 'https://191.231.62.142:8000'
        }

        self.urls = []

    # UA pool
    def LoadUA(self, uafile):
        ua_list = []
        with open(uafile, 'rb') as ua_f:
            for ua in ua_f.readlines():
                if ua:
                    ua_list.append(ua.strip())
        return ua_list

    def get_page(self):

        # note: can select other ranges, but some may not work
        for m in range(5214, 5215):

            # url format: https://space.bilibili.com/####
            for i in range(m * 100, (m + 1) * 100):
                url = 'https://space.bilibili.com/' + str(i)
                self.urls.append(url)

    def json_parse(self, url):

        web_id = url[-6:]

        payload = {
            'mid': url.replace('https://space.bilibili.com/', '')
        }

        head = {
            'User-Agent': random.choice(self.ua_list),
            'Referer': 'https://space.bilibili.com/' + str(web_id) + '?from=search&seid=' + str(random.randint(10000, 50000))
        }

        mid = payload['mid']

        jscontent = requests.session().get(
            'https://api.bilibili.com/x/space/acc/info?mid=%s&jsonp=jsonp' % mid,
            headers=head,
            data=payload
        ).text

        try:
            jsDict = json.loads(jscontent)  # convert json content to python object
            print(jsDict)
            print('--------------------------------------------')
            status_code = jsDict['code'] if 'code' in jsDict.keys() else False  # false if request rejected

            if status_code == 0:
                self.sql(jsDict)
            else:
                print("Error: " + url)  # cannot request html content

        except Exception as e:
            print(e)
            pass

    def sql(self, jsDict):

        if 'data' in jsDict.keys():
            # MySQL table columns
            jsData = jsDict['data']
            mid = jsData['mid']
            name = jsData['name']
            sex = jsData['sex']
            rank = jsData['rank']
            face = jsData['face']
            regtimestamp = jsData['jointime']
            regtime_local = time.localtime(regtimestamp)
            regtime = time.strftime("%Y-%m-%d %H:%M:%S", regtime_local)
            birthday = jsData['birthday'] if 'birthday' in jsData.keys() else 'nobirthday'
            sign = jsData['sign']
            level = jsData['level']
            OfficialVerifyType = jsData['official']['type']
            OfficialVerifyDesc = jsData['official']['desc']
            vipType = jsData['vip']['type']
            vipStatus = jsData['vip']['status']
            coins = jsData['coins']
            print("Succeed get user info: " + str(mid) + "\t")
            try:
                res = requests.get(
                    'https://api.bilibili.com/x/relation/stat?vmid=' + str(mid) + '&jsonp=jsonp').text
                js_fans_data = json.loads(res)
                following = js_fans_data['data']['following']
                fans = js_fans_data['data']['follower']
            except:
                following = 0
                fans = 0

        else:
            print('no data')
        try:
            # MySQL manipulation
            conn = pymysql.connect(
                host='localhost', user='root', passwd='Leo20020927', db='bilibili', charset='utf8')
            cur = conn.cursor()
            cur.execute('''
            INSERT INTO bilibili_user_info(mid, name, sex, `rank`, face, regtime, birthday, sign, level, OfficialVerifyType, OfficialVerifyDesc, vipType, vipStatus, coins, following, fans)
            VALUES ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")
            '''
            ,
            (mid, name, sex, rank, face, regtime, birthday, sign, level, OfficialVerifyType,
             OfficialVerifyDesc, vipType, vipStatus, coins, following, fans))
            conn.commit()
            print("succeed sending user info to SQL database")
        except Exception as e:
            print(e)

    def main(self):

        time1 = time.time()  # record time before crawling data
        self.get_page()
        pool = ThreadPool(1)
        try:
            results = pool.map(self.json_parse, self.urls)
        except Exception as e:
            print(e)

        time2 = time.time()  # record time after sending html request
        print("time used: " + str(time2 - time1))

        pool.close()
        pool.join()


if __name__ == "__main__":

    crawler = bilibili_crawler()
    crawler.main()
