# 1、第一个因子是价格，120以下的可转债是首选。
# 2、第二个因子是日K的极差比率，每一根日K线的低点至高点之间的涨幅，这个指标反映了标的的日内波动程度。
# 3、第三个因子是变异系数，日K线的变异系数其含义是每日收盘价序列的标准差与均值的比，表征着标的在日线级别上的波动程度，同样，该指标没有量纲，方便标的横向对比。
# 接下的思路就很简单了，标的选取原则是：在价格足够低的前提下，选取日K极差比率大，变异系数大的交易标的。当然还有很多其他因子要考虑，比如剩余期限，溢价率之类的，这个可以在前述因子筛选后再进行考量。
from domain.BondBasicInfo import getBaseInfoList
from domain.BondQuoteHistory import getQuoteHistoryList, getQuoteHistoriesBtDataByCode
import backtrader as bt
import pandas as pd
import datetime
import json
import efinance as ef


def get_k_data(bond_code, begin: datetime, end: datetime) -> pd.DataFrame:
    """
    根据efinance工具包获取股票数据
    :param stock_code:股票代码
    :param begin: 开始日期
    :param end: 结束日期
    :return:
    """
    # stock_code = '600519'  # 股票代码，茅台
    k_dataframe: pd.DataFrame = ef.bond.get_quote_history(
        bond_code, beg=begin.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"))
    k_dataframe = k_dataframe.iloc[:, :9]
    k_dataframe.columns = ['name', 'code', 'date', 'open', 'close', 'high', 'low', 'volume', 'turnover']
    k_dataframe.index = pd.to_datetime(k_dataframe.date)
    k_dataframe.drop(['name', 'code', "date"], axis=1, inplace=True)
    return k_dataframe

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


class PandasDataPlus(bt.feeds.PandasData):
    lines = ('turnover',)  # 要添加的列名
    # 设置 line 在数据源上新增的位置
    params = (
        ('turnover', -1),  # turnover对应传入数据的列名，这个-1会自动匹配backtrader的数据类与原有pandas文件的列名
        # 如果是个大于等于0的数，比如8，那么backtrader会将原始数据下标8(第9列，下标从0开始)的列认为是turnover这一列
    )


class MyStrategy1(bt.Strategy):  # 策略
    def __init__(self):
        # 初始化交易指令、买卖价格和手续费
        self.close_price = self.datas[0].close  # 这里加一个数据引用，方便后续操作
        this_data = self.getdatabyname("110088")  # 获取传入的 name = 110088 的数据
        print("全部列名：", this_data.getlinealiases())  # 全部的列名称
        print("总交易日：", self.datas[0].buflen())  # 数据集中一共有多少行

    def next(self):  # 框架执行过程中会不断循环next()，过一个K线，执行一次next()
        print('==========================')
        print("今日{}, 是第{}个交易日 , 收盘价：{}".format(self.datetime.date(), len(self.datas[0]), self.datas[0].close[0]))
        print("前天、昨天、今天的收盘价：", list(self.datas[0].close.get(ago=0, size=3)))  # 使用 get() 向前获取数据
        if len(self.datas[0]) <= self.datas[0].buflen() - 2:
            print("明天、后天的收盘价：", self.datas[0].close[1], self.datas[0].close[2])


def getBt():
    # 获取数据
    df = getQuoteHistoriesBtDataByCode('110088')
    start_time = datetime.datetime(2020, 1, 1)
    end_time = datetime.datetime(2023, 8, 10)

    # =============== 为系统注入数据 =================
    data = PandasDataPlus(dataname=df, fromdate=start_time, todate=end_time)
    # 初始化cerebro回测系统
    cerebro = bt.Cerebro()  # Cerebro引擎在后台创建了broker(经纪人)实例，系统默认每个broker的初始资金量为10000
    # 将数据传入回测系统
    cerebro.adddata(data, name="110088")  # 导入数据，在策略中使用 self.datas 来获取数据源
    # 将交易策略加载到回测系统中
    cerebro.addstrategy(MyStrategy1)
    # =============== 系统设置 ==================
    # 引擎运行前打印期出资金
    print('组合期初资金: %.2f' % cerebro.broker.getvalue())
    # 运行回测系统
    cerebro.run()
    # 引擎运行后打期末资金
    print('组合期末资金: %.2f' % cerebro.broker.getvalue())
    cerebro.plot()


getBt()
