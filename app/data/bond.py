import datetime
from domain.BondQuoteHistory import mainDownQuoteHistoryAndUpdate
from domain.BondBasicInfo import downBaseInfoAndUpdate
from domain.BaseMixin import createTable
from superstream import Stream


def updateBaseInfoAndHistory():
    """
    更新基础信息表（所有债券）和所有债券对应的历史信息
    :return:
    """
    # 1、获取全部债券基本信息列表
    listBaseInfos = downBaseInfoAndUpdate()

    # 2、更新已上市的列表，所有的历史详情
    # 2、1过滤已上市的债券代码
    today = datetime.date.today()
    toUpdateCodes = Stream(listBaseInfos).filter(
        lambda x: x['listing_date'] is not None and x['listing_date'] <= str(today) < x['expiry_date']
    ).map(lambda x: x['bond_code']).to_list()

    # 3、批量异步更新历史详情
    mainDownQuoteHistoryAndUpdate(toUpdateCodes)


if __name__ == '__main__':
    # 初始化表 创建表
    createTable()
    # 获取所有债券信息和债券的历史交易信息
    updateBaseInfoAndHistory()
