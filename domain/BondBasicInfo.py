import efinance as ef
import datetime
from domain.BaseMixin import BaseMixin, Base, toColumns, getSession
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import (
    Column,
    Double,
    Date,
    String,
    UniqueConstraint,
    Integer,
    Text,
)
from superstream import Stream


class BondBasicInfo(Base, BaseMixin):
    __tablename__ = "bond_basic_info"
    bondCode = Column("bond_code", String(10), nullable=False, comment="债券代码", index=True)
    bondName = Column("bond_name", String(10), nullable=False, comment="债券名称", index=True)
    stockCode = Column("stock_code", String(10), nullable=False, comment="正股代码", index=True)
    sdockName = Column("stock_name", String(10), nullable=False, comment="正股名称", index=True)
    bondRating = Column("bond_rating", String(10), nullable=False, comment="债券评级")
    subscriptDate = Column("subscript_date", Date, nullable=False, comment="申购日期")
    issueScale = Column("issue_scale", Double, nullable=False, comment="发行规模(亿)")
    issuanceSuccessRate = Column("issuance_success_rate", Double, comment="网上发行中签率(%)")
    listingDate = Column("listing_date", Date, comment="上市日期")
    expiryDate = Column("expiry_date", Date, nullable=False, comment="到期日期")
    term = Column("term", Integer, nullable=False, comment="期限(年)")
    interestRateDescription = Column("interest_rate_description", Text, nullable=False, comment="利率说明")

    __table_args__ = (
        # 联合唯一索引
        UniqueConstraint('bond_code', name='idx_uni_code'),
    )


def updateBatchBaseInfo(listIn):
    """
    更新基础信息表
    :param listIn:
    :return:
    """
    if not listIn:
        return
    with getSession() as s:
        # 更新或者插入
        stmt = insert(BondBasicInfo).values(listIn)
        do_update_stmt = stmt.on_duplicate_key_update(
            # 唯一索引
            bond_code=stmt.inserted.bond_code,
        )
        s.execute(do_update_stmt)


def downBaseInfoAndUpdate():
    """
    获取全部债券基本信息列表并入库
    :return:
    """
    bondBaseInfos = ef.bond.get_all_base_info().rename(columns=toColumns(BondBasicInfo))
    bondBaseInfos = bondBaseInfos.astype(object).where(bondBaseInfos.notna(), None)
    listBaseInfos = bondBaseInfos.to_dict("records")
    # 1、1插入或更新基础信息
    updateBatchBaseInfo(listBaseInfos)
    return listBaseInfos


def getBaseInfoList(bondRating=None, issueScaleStart=None, issueScaleEnd=None):
    """
    查询已上市流通的
    :param bondRating: 评级
    :param issueScaleStart: 规模范围
    :param issueScaleEnd: 规模范围
    :return:
    """
    # 上市、流通
    today = datetime.date.today()
    queries = [BondBasicInfo.listingDate < str(today), BondBasicInfo.expiryDate > str(today)]
    if bondRating:
        queries.append(BondBasicInfo.bondRating.in_(bondRating))
    if issueScaleStart:
        queries.append(BondBasicInfo.issueScale >= issueScaleStart)
    if issueScaleEnd:
        queries.append(BondBasicInfo.issueScale <= issueScaleEnd)
    with getSession() as s:
        listBasicInfo = s.query(BondBasicInfo).filter(*queries).all()
        return [x.bondCode for x in listBasicInfo]

getBaseInfoList()