# # -*- coding: utf-8 -*-

# 数据爬取文件

import scrapy
import pymysql
import pymssql
from ..items import JinanlvyouItem
import time
from datetime import datetime,timedelta
import datetime as formattime
import re
import random
import platform
import json
import os
import urllib
from urllib.parse import urlparse
import requests
import emoji
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from selenium.webdriver import ChromeOptions, ActionChains
from scrapy.http import TextResponse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
# 济南旅游
class JinanlvyouSpider(scrapy.Spider):
    name = 'jinanlvyouSpider'
    spiderUrl = 'https://travel.qunar.com/search/place/22-jinan-300150/0-----0/{}'
    start_urls = spiderUrl.split(";")
    protocol = ''
    hostname = ''
    realtime = False

    headers = {
        "Cookie":""
    }

    def __init__(self,realtime=False,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.realtime = realtime=='true'

    def start_requests(self):

        plat = platform.system().lower()
        if not self.realtime and (plat == 'linux' or plat == 'windows'):
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, '9mk9u946_jinanlvyou') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return
        pageNum = 1 + 1

        for url in self.start_urls:
            if '{}' in url:
                for page in range(1, pageNum):

                    next_link = url.format(page)
                    yield scrapy.Request(
                        url=next_link,
                        headers=self.headers,
                        callback=self.parse
                    )
            else:
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    callback=self.parse
                )

    # 列表解析
    def parse(self, response):
        _url = urlparse(self.spiderUrl)
        self.protocol = _url.scheme
        self.hostname = _url.netloc
        plat = platform.system().lower()
        if not self.realtime and (plat == 'linux' or plat == 'windows'):
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, '9mk9u946_jinanlvyou') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return
        list = response.css('ul.b_destlist li.list_item')
        for item in list:
            fields = JinanlvyouItem()

            if '(.*?)' in '''a.tit::text''':
                try:
                    fields["title"] = str( re.findall(r'''a.tit::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["title"] = str( self.remove_html(item.css('''a.tit::text''').extract_first()))

                except:
                    pass
            if '(.*?)' in '''a.d_img img::attr(src)''':
                try:
                    fields["picture"] = str( re.findall(r'''a.d_img img::attr(src)''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["picture"] = str( self.remove_html(item.css('''a.d_img img::attr(src)''').extract_first()))

                except:
                    pass
            if '(.*?)' in '''div.d_days::text''':
                try:
                    fields["wandays"] = str( re.findall(r'''div.d_days::text''', item.extract(), re.DOTALL)[0].strip().replace("建议游玩时间：",""))

                except:
                    pass
            else:
                try:
                    fields["wandays"] = str( self.remove_html(item.css('''div.d_days::text''').extract_first()).replace("建议游玩时间：",""))

                except:
                    pass
            if '(.*?)' in '''div.d_brief::text''':
                try:
                    fields["brief"] = str( re.findall(r'''div.d_brief::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["brief"] = str( self.remove_html(item.css('''div.d_brief::text''').extract_first()))

                except:
                    pass
            if '(.*?)' in '''div.d_address::text''':
                try:
                    fields["address"] = str( re.findall(r'''div.d_address::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["address"] = str( self.remove_html(item.css('''div.d_address::text''').extract_first()))

                except:
                    pass
            if '(.*?)' in '''div.e_img a::attr(href)''':
                try:
                    fields["laiyuan"] = str( re.findall(r'''div.e_img a::attr(href)''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["laiyuan"] = str( self.remove_html(item.css('''div.e_img a::attr(href)''').extract_first()))

                except:
                    pass
            detailUrlRule = item.css('div.e_img a::attr(href)').extract_first()
            if self.protocol in detailUrlRule:
                pass
            elif detailUrlRule.startswith('//'):
                detailUrlRule = self.protocol + ':' + detailUrlRule
            elif detailUrlRule.startswith('/'):
                detailUrlRule = self.protocol + '://' + self.hostname + detailUrlRule
                # fields["laiyuan"] = detailUrlRule
            else:
                detailUrlRule = self.protocol + '://' + self.hostname + '/' + detailUrlRule
            yield scrapy.Request(url=detailUrlRule, meta={'fields': fields}, headers=self.headers, callback=self.detail_parse, dont_filter=True)

    # 详情解析
    def detail_parse(self, response):
        fields = response.meta['fields']
        try:
            if '(.*?)' in '''span.cur_score::text''':
                fields["score"] = float( re.findall(r'''span.cur_score::text''', response.text, re.S)[0].strip())
            else:
                if 'score' != 'xiangqing' and 'score' != 'detail' and 'score' != 'pinglun' and 'score' != 'zuofa':
                    fields["score"] = float( self.remove_html(response.css('''span.cur_score::text''').extract_first()))
                else:
                    try:
                        fields["score"] = float( emoji.demojize(response.css('''span.cur_score::text''').extract_first()))
                    except:
                        pass
        except:
            pass
        try:
            if '(.*?)' in '''div.txtbox span.sum::text''':
                fields["ranking"] = int( re.findall(r'''div.txtbox span.sum::text''', response.text, re.S)[0].strip())
            else:
                if 'ranking' != 'xiangqing' and 'ranking' != 'detail' and 'ranking' != 'pinglun' and 'ranking' != 'zuofa':
                    fields["ranking"] = int( self.remove_html(response.css('''div.txtbox span.sum::text''').extract_first()))
                else:
                    try:
                        fields["ranking"] = int( emoji.demojize(response.css('''div.txtbox span.sum::text''').extract_first()))
                    except:
                        pass
        except:
            pass
        try:
            if '(.*?)' in '''div.txtbox div.ranking a::text''':
                fields["renshu"] = int( re.findall(r'''div.txtbox div.ranking a::text''', response.text, re.S)[0].strip().replace("共","").replace("个",""))
            else:
                if 'renshu' != 'xiangqing' and 'renshu' != 'detail' and 'renshu' != 'pinglun' and 'renshu' != 'zuofa':
                    fields["renshu"] = int( self.remove_html(response.css('''div.txtbox div.ranking a::text''').extract_first()).replace("共","").replace("个",""))
                else:
                    try:
                        fields["renshu"] = int( emoji.demojize(response.css('''div.txtbox div.ranking a::text''').extract_first()).replace("共","").replace("个",""))
                    except:
                        pass
        except:
            pass
        try:
            if '(.*?)' in '''div.b_detail_section.b_detail_summary div.e_db_content_box p::text''':
                fields["gaishu"] = str( re.findall(r'''div.b_detail_section.b_detail_summary div.e_db_content_box p::text''', response.text, re.S)[0].strip().replace(" ",""))

            else:
                fields["gaishu"] = ' '.join(response.css('''div.b_detail_section.b_detail_summary div.e_db_content_box p::text''').extract()).replace(" ","")
        except:
            pass
        try:
            if '(.*?)' in '''div.b_detail_section.b_detail_ticket div.e_db_content_box.e_db_content_dont_indent>p::text''':
                fields["ticket"] = float( re.findall(r'''div.b_detail_section.b_detail_ticket div.e_db_content_box.e_db_content_dont_indent>p::text''', response.text, re.S)[0].strip().replace("元",""))
            else:
                if 'ticket' != 'xiangqing' and 'ticket' != 'detail' and 'ticket' != 'pinglun' and 'ticket' != 'zuofa':
                    fields["ticket"] = float( self.remove_html(response.css('''div.b_detail_section.b_detail_ticket div.e_db_content_box.e_db_content_dont_indent>p::text''').extract_first()).replace("元",""))
                else:
                    try:
                        fields["ticket"] = float( emoji.demojize(response.css('''div.b_detail_section.b_detail_ticket div.e_db_content_box.e_db_content_dont_indent>p::text''').extract_first()).replace("元",""))
                    except:
                        pass
        except:
            pass
        return fields

    # 数据清洗
    def pandas_filter(self):
        engine = create_engine('mysql+pymysql://root:123456@localhost/spider9mk9u946?charset=UTF8MB4')
        df = pd.read_sql('select * from jinanlvyou limit 50', con = engine)

        # 重复数据过滤
        df.duplicated()
        df.drop_duplicates()

        #空数据过滤
        df.isnull()
        df.dropna()

        # 填充空数据
        df.fillna(value = '暂无')

        # 异常值过滤

        # 滤出 大于800 和 小于 100 的
        a = np.random.randint(0, 1000, size = 200)
        cond = (a<=800) & (a>=100)
        a[cond]

        # 过滤正态分布的异常值
        b = np.random.randn(100000)
        # 3σ过滤异常值，σ即是标准差
        cond = np.abs(b) > 3 * 1
        b[cond]

        # 正态分布数据
        df2 = pd.DataFrame(data = np.random.randn(10000,3))
        # 3σ过滤异常值，σ即是标准差
        cond = (df2 > 3*df2.std()).any(axis = 1)
        # 不满⾜条件的⾏索引
        index = df2[cond].index
        # 根据⾏索引，进⾏数据删除
        df2.drop(labels=index,axis = 0)

    # 去除多余html标签
    def remove_html(self, html):
        if html == None:
            return ''
        pattern = re.compile(r'<[^>]+>', re.S)
        return pattern.sub('', html).strip()

    # 数据库连接
    def db_connect(self):
        type = self.settings.get('TYPE', 'mysql')
        host = self.settings.get('HOST', 'localhost')
        port = int(self.settings.get('PORT', 3306))
        user = self.settings.get('USER', 'root')
        password = self.settings.get('PASSWORD', '123456')

        try:
            database = self.databaseName
        except:
            database = self.settings.get('DATABASE', '')

        if type == 'mysql':
            connect = pymysql.connect(host=host, port=port, db=database, user=user, passwd=password, charset='utf8')
        else:
            connect = pymssql.connect(host=host, user=user, password=password, database=database)
        return connect

    # 断表是否存在
    def table_exists(self, cursor, table_name):
        cursor.execute("show tables;")
        tables = [cursor.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]

        if table_name in table_list:
            return 1
        else:
            return 0

    # 数据缓存源
    def temp_data(self):

        connect = self.db_connect()
        cursor = connect.cursor()
        sql = '''
            insert into `jinanlvyou`(
                id
                ,title
                ,picture
                ,wandays
                ,brief
                ,address
                ,score
                ,ranking
                ,renshu
                ,gaishu
                ,laiyuan
                ,ticket
            )
            select
                id
                ,title
                ,picture
                ,wandays
                ,brief
                ,address
                ,score
                ,ranking
                ,renshu
                ,gaishu
                ,laiyuan
                ,ticket
            from `9mk9u946_jinanlvyou`
            where(not exists (select
                id
                ,title
                ,picture
                ,wandays
                ,brief
                ,address
                ,score
                ,ranking
                ,renshu
                ,gaishu
                ,laiyuan
                ,ticket
            from `jinanlvyou` where
                `jinanlvyou`.id=`9mk9u946_jinanlvyou`.id
            ))
        '''

        cursor.execute(sql)
        connect.commit()
        connect.close()
