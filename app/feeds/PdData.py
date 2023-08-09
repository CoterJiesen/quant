import backtrader as bt


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
