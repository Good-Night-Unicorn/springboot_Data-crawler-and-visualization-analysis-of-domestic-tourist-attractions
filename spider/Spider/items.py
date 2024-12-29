# 数据容器文件

import scrapy

class SpiderItem(scrapy.Item):
    pass

class JinanlvyouItem(scrapy.Item):
    # 标题
    title = scrapy.Field()
    # 图片
    picture = scrapy.Field()
    # 建议游玩时间
    wandays = scrapy.Field()
    # 简介
    brief = scrapy.Field()
    # 地址
    address = scrapy.Field()
    # 评分
    score = scrapy.Field()
    # 济南景点排名
    ranking = scrapy.Field()
    # 打分人数
    renshu = scrapy.Field()
    # 概述
    gaishu = scrapy.Field()
    # 来源
    laiyuan = scrapy.Field()
    # 门票
    ticket = scrapy.Field()

