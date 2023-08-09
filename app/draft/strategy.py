# 1、第一个因子是价格，120以下的可转债是首选。
# 2、第二个因子是日K的极差比率，每一根日K线的低点至高点之间的涨幅，这个指标反映了标的的日内波动程度。
# 3、第三个因子是变异系数，日K线的变异系数其含义是每日收盘价序列的标准差与均值的比，表征着标的在日线级别上的波动程度，同样，该指标没有量纲，方便标的横向对比。
# 接下的思路就很简单了，标的选取原则是：在价格足够低的前提下，选取日K极差比率大，变异系数大的交易标的。当然还有很多其他因子要考虑，比如剩余期限，溢价率之类的，这个可以在前述因子筛选后再进行考量。
from domain.BondBasicInfo import getBaseInfoList
from domain.BondQuoteHistory import getQuoteHistoryList,getQuoteHistoriesBtDataByCode
import backtrader as bt
import pandas as pd
import datetime
import json


class TusharePdData(bt.feeds.PandasData):
    '''
    从Tushare读取A股票数据日线
    '''
    params = (
        ('datetime', 2),
        ('open', 3),
        ('high', 5),
        ('low', 6),
        ('close', 4),
        ('volume', 7),
        ('openinterest', -1),
    )


def getStrategyList():
    """
    根据策略获取
    :return:
    """
    # 评级
    bondRating = ['AA+', 'AAA']
    # 规模小于
    issueScaleEnd = 100
    # 价格
    closing = 120

    # 1、获取满足评级、规模的代码
    codes = getBaseInfoList(bondRating=bondRating, issueScaleEnd=issueScaleEnd)
    # 2、获取满足价格的列表
    df = getQuoteHistoryList(codes=codes, closing=closing)
    # df.index = pd.to_datetime(df.date, format='%Y%m%d')
    print(df)

    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    # Cerebro引擎在后台创建broker(经纪人)，系统默认资金量为10000
    # 设置投资金额100000.0
    cerebro.broker.setcash(100000.0)
    # 读取和导入 dataframe 数据框 - 方式2
    start = "20190101"
    end = "20191231"
    dt_start = datetime.datetime.strptime(start, "%Y%m%d")
    dt_end = datetime.datetime.strptime(end, "%Y%m%d")
    data = TusharePdData(df, fromdate=dt_start, todate=dt_end)
    # data = bt.feeds.PandasDirectData(dataname=df)
    cerebro.adddata(data, name='XXX')

    # 引擎运行前打印期出资金
    print('组合期初资金: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    # 引擎运行后打期末资金
    print('组合期末资金: %.2f' % cerebro.broker.getvalue())


getStrategyList()

