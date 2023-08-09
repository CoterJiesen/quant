import asyncio
import efinance as ef
import numpy as np
import datetime
import pandas as pd
from domain.BaseMixin import BaseMixin, Base, toColumns, toBtColumnsAll, getSession, toJson, key2list
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql.expression import func
from sqlalchemy import (
    Column,
    Double,
    Date,
    String,
    UniqueConstraint,
)
from superstream import Stream
from sqlalchemy.orm import aliased

# 缓存最新的一条历史记录
globalCacheListNewestBondQuoteHistory = {'cached': False, 'dictBondQuoteHistory': {}}


class BondQuoteHistory(Base, BaseMixin):
    __tablename__ = "bond_quote_history"
    bondCode = Column("bond_code", String(10), nullable=False, comment="债券代码", index=True)
    bondName = Column("bond_name", String(10), nullable=False, comment="债券名称", index=True)
    date = Column(Date, nullable=False, comment="日期", index=True)
    opening = Column(Double, nullable=False, comment="开盘")
    closing = Column(Double, nullable=False, comment="收盘")
    highest = Column(Double, nullable=False, comment="最高")
    lowest = Column(Double, nullable=False, comment="最低")
    tradingVolume = Column("trading_volume", Double, nullable=False, comment="成交量")
    turnover = Column(Double, nullable=False, comment="成交额")
    amplitude = Column(Double, nullable=False, comment="振幅")
    change = Column(Double, nullable=False, comment="涨跌幅")
    changeAmount = Column("change_amount", Double, nullable=False, comment="涨跌额")
    turnoverRate = Column("turnover_rate", Double, nullable=False, comment="换手率")
    mid = Column(Double, comment="波动率中位数")
    cov = Column(Double, comment="变异系数")

    __table_args__ = (
        # 联合唯一索引
        UniqueConstraint('bond_code', 'date', name='ix_bond_quote_history_bond_code_bond_date'),
    )

    def toDict(self):  # 方法二，该方法可以将获取结果进行定制，例如如下是将所有非空值输出成str类型
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                if type(getattr(self, key)) == datetime.datetime or type(getattr(self, key)) == datetime.date:
                    result[key] = str(getattr(self, key))
                else:
                    result[key] = getattr(self, key)
        return result

    Base.toDict = toDict  # 如果使用的是flask-sqlalchemy，就使用对应的基类


def getQuoteHistoryByCode(code):
    """
    根据代码查找对应最新的更新日期
    :param code:
    :return:
    """
    if not globalCacheListNewestBondQuoteHistory.get('cached'):
        with getSession() as s:
            listBQH = s.query(BondQuoteHistory.bondCode, BondQuoteHistory.date,
                              func.max(BondQuoteHistory.date)).group_by(
                BondQuoteHistory.bondCode).all()
            dictMaxDate = {}
            for row in listBQH:
                dictMaxDate[row.bondCode] = row.date
            globalCacheListNewestBondQuoteHistory['cached'] = True
            globalCacheListNewestBondQuoteHistory['listBondQuoteHistory'] = dictMaxDate
    dictBQH = globalCacheListNewestBondQuoteHistory.get('listBondQuoteHistory')
    return dictBQH[code] if (code in dictBQH) else None


def updateBatchQuoteHistoryByCode(code, listIn):
    """
    根据代码更新存量数据最新日期之后的数据
    :param code:
    :param listIn: 所有数据
    :return:
    """
    if not listIn or not code:
        return
    with getSession() as s:
        today = getQuoteHistoryByCode(code)
        updateList = []
        if today is not None:
            updateList = Stream(listIn).filter(lambda x: x['date'] >= str(today)).to_list()
        if today is None or len(updateList) == 0:
            updateList = listIn
        if len(updateList) != 0:
            # 更新或者插入
            stmt = insert(BondQuoteHistory).values(updateList)
            do_update_stmt = stmt.on_duplicate_key_update(
                # 以这两个值为唯一索引
                date=stmt.inserted.date,
                bond_code=stmt.inserted.bond_code,
            )
            s.execute(do_update_stmt)


async def taskDownQuoteHistoryAndUpdate(code):
    # 2、2 根据代码获取历史详情
    bondQuoteHistories = ef.bond.get_quote_history(code).rename(columns=toColumns(BondQuoteHistory))
    bondQuoteHistories = clcCovAll(bondQuoteHistories)
    listQuoteHistories = bondQuoteHistories.to_dict("records")
    # 2、3 更新历史详情
    updateBatchQuoteHistoryByCode(code, listQuoteHistories)


async def taskPoolDownQuoteHistoryAndUpdate(codes):
    tasks = [asyncio.create_task(taskDownQuoteHistoryAndUpdate(v)) for v in codes]
    await asyncio.wait(tasks)


def mainDownQuoteHistoryAndUpdate(codes):
    # 提前先缓存代码对应更新到的最新截止日期
    getQuoteHistoryByCode("666666")
    asyncio.run(taskPoolDownQuoteHistoryAndUpdate(codes))


def clcCovAll(dfQuoteHistory):
    for index, row in dfQuoteHistory.iterrows():
        rowDate = row['date']
        dfTp = dfQuoteHistory[dfQuoteHistory['date'] < rowDate]
        mid, cov = clcCovOne(dfTp)
        dfQuoteHistory.loc[index, 'mid'] = mid
        dfQuoteHistory.loc[index, 'cov'] = cov
    return dfQuoteHistory


def clcCovOne(dfQuoteHistory):
    """
    1、根据历史最高、最低值计算当前的中位数
    2、根据历史收盘价计算变异系数（当前日期以前的标准差/平均值）
    :param dfQuoteHistoryList:
    :return:
    """
    if len(dfQuoteHistory) <= 0:
        return 0, 0
    rangeMid = dfQuoteHistory['highest'] / dfQuoteHistory['lowest'] - 1
    mid = np.percentile(rangeMid, 50)
    cov = np.std(dfQuoteHistory['closing']) / np.mean(dfQuoteHistory['closing'])
    return mid, cov


# df = ef.bond.get_quote_history('128116').rename(columns=toColumns(BondQuoteHistory))
# clcCovAll(df)


def getQuoteHistoriesBtDataByCode(code):
    """
    根据代码查找对应历史交易信息bt数据
    :param code:
    :return:
    """
    with getSession() as s:
        listBQH = s.query(BondQuoteHistory).filter(BondQuoteHistory.bondCode == code).all()
        df = pd.DataFrame.from_records([s.toDict() for s in listBQH])
        k_dataframe = df.iloc[:, :9]
        k_dataframe.columns = ['code', 'name', 'date', 'open', 'close', 'high', 'low', 'volume', 'turnover']
        k_dataframe.index = pd.to_datetime(k_dataframe.date)
        k_dataframe.drop(['name', 'code', "date"], axis=1, inplace=True)
        return k_dataframe


def getQuoteHistoryList(closing=None, codes=None):
    """
    根据代码查找对应最新的更新日期
    :param code:
    :return:
    """
    with getSession() as s:
        """
        SELECT
            t.* 
        FROM
            ( SELECT * FROM bond_quote_history ORDER BY date DESC LIMIT 100000000 ) AS t 
        GROUP BY
            t.bond_code;
        """
        # 子查询
        subquery = s.query(BondQuoteHistory).order_by(BondQuoteHistory.date.desc()).limit(
            100000000).subquery()

        # 查询条件
        queries = []
        if codes:
            queries.append(subquery.c.bond_code.in_(codes))
        if closing:
            queries.append(subquery.c.closing <= closing)

        # 查询
        listBQH = s.query(subquery.c).filter(*queries).group_by(subquery.c.bond_code).all()
        # listBQH = s.query(BondQuoteHistory, func.max(BondQuoteHistory.date)).filter(*queries).group_by(
        #     BondQuoteHistory.bondCode).all()
        # updateList = Stream(listBQH).map(lambda x: x[0]).to_list()
        # 取字段名为列名
        # variables = list(listBQH[0].__dict__.keys())
        # variables.remove('_sa_instance_state')
        # df = pd.DataFrame([[getattr(i, j) for j in variables] for i in listBQH], columns=variables)
        # 将日期列，设置成index
        return pd.DataFrame(listBQH, columns=toBtColumnsAll(BondQuoteHistory))


getQuoteHistoryList()
