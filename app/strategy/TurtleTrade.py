from __future__ import (absolute_import, division, print_function, unicode_literals)
import datetime
import backtrader as bt
from domain.BondQuoteHistory import getQuoteHistoriesBtDataByCode
from app.feeds.PdData import PandasDataPlus


class TestSizer(bt.Sizer):
    params = (('stake', 1),)

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            return self.p.stake
        position = self.broker.getposition(data)
        if not position.size:
            return 0
        else:
            return position.size
        return self.p.stake


class TestStrategy(bt.Strategy):
    params = (('maperiod', 15), ('printlog', False),)

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):

        self.dataClose = self.datas[0].close
        self.dataHigh = self.datas[0].high
        self.dataLow = self.datas[0].low

        self.order = None
        self.buyPrice = 0
        self.buyComm = 0
        self.newStake = 0
        self.buyTime = 0
        # 参数计算，唐奇安通道上轨、唐奇安通道下轨、ATR
        self.DonchianHi = bt.indicators.Highest(self.dataHigh(-1), period=20, subplot=False)
        self.DonchianLo = bt.indicators.Lowest(self.dataLow(-1), period=10, subplot=False)
        self.TR = bt.indicators.Max((self.dataHigh(0) - self.dataLow(0)), abs(self.dataClose(-1) - self.dataHigh(0)),
                                    abs(self.dataClose(-1) - self.dataLow(0)))
        self.ATR = bt.indicators.SimpleMovingAverage(self.TR, period=14, subplot=True)
        # 唐奇安通道上轨突破、唐奇安通道下轨突破
        self.CrossoverHi = bt.ind.CrossOver(self.dataClose(0), self.DonchianHi)
        self.CrossoverLo = bt.ind.CrossOver(self.dataClose(0), self.DonchianLo)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm), doprint=True)
                self.buyPrice = order.executed.price
                self.buyComm = order.executed.comm
            else:
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm), doprint=True)
                self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' % (trade.pnl, trade.pnlcomm))

    def next(self):
        if self.order:
            return
        # 入场
        if self.CrossoverHi > 0 and self.buyTime == 0:
            self.newStake = self.broker.getvalue() * 0.01 / self.ATR
            self.newStake = int(self.newStake / 100) * 100
            self.sizer.p.stake = self.newStake
            self.buyTime = 1
            self.order = self.buy()
        # 加仓
        elif self.datas[0].close > self.buyPrice + 0.5 * self.ATR[0] and self.buyTime > 0 and self.buyTime < 5:
            self.newStake = self.broker.getvalue() * 0.01 / self.ATR
            self.newStake = int(self.newStake / 100) * 100
            self.sizer.p.stake = self.newStake
            self.order = self.buy()
            self.buyTime = self.buyTime + 1
        # 出场
        elif self.CrossoverLo < 0 and self.buyTime > 0:
            self.order = self.sell()
            self.buyTime = 0
        # 止损
        elif self.datas[0].close < (self.buyPrice - 2 * self.ATR[0]) and self.buyTime > 0:
            self.order = self.sell()
            self.buyTime = 0

    def stop(self):
        self.log('(MA Period %2d) Ending Value %.2f' % (self.params.maperiod, self.broker.getvalue()), doprint=True)


if __name__ == '__main__':
    code = '110088'
    # 创建主控制器
    cerebro = bt.Cerebro()
    # 加入策略
    cerebro.addstrategy(TestStrategy)
    # 准备股票日线数据，输入到backtrader
    # 获取数据
    df = getQuoteHistoriesBtDataByCode(code)
    start_time = datetime.datetime(2020, 1, 1)
    end_time = datetime.datetime(2023, 8, 10)
    data = PandasDataPlus(dataname=df, fromdate=start_time, todate=end_time)
    # =============== 为系统注入数据 =================
    cerebro.adddata(data, name=code)
    # broker设置资金、手续费
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    # 设置买入策略
    cerebro.addsizer(TestSizer)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # 启动回测
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # 曲线绘图输出
    cerebro.plot()
