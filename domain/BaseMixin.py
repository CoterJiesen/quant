import datetime
import contextlib
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    DateTime,
    String,
)
from config.Config import mysql  # config模块里有自己写的配置，我们可以换成别的，注意下面用到config的地方也要一起换

engine = create_engine(
    mysql["DATABASE_URI"],  # SQLAlchemy 数据库连接串，格式见下面
    echo=bool(mysql["ECHO"]),  # 是不是要把所执行的SQL打印出来，一般用于调试
    pool_size=int(mysql["POOL_SIZE"]),  # 连接池大小
    max_overflow=int(mysql["POOL_MAX_SIZE"]),  # 连接池最大的大小
    pool_recycle=int(mysql["POOL_RECYCLE"]),  # 多久时间主动回收连接，见下注释
    pool_timeout=int(mysql["POOL_TIMEOUT"])
)
Session = sessionmaker(bind=engine)
Base = declarative_base()


def toDict(self):
    return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


Base.toDict = toDict


class BaseMixin:
    """model的基类,所有model都必须继承"""
    id = Column(Integer, primary_key=True)
    createdAt = Column("created_at", DateTime, nullable=False, default=datetime.datetime.now)
    updatedAt = Column("updated_at", DateTime, nullable=False, default=datetime.datetime.now,
                       onupdate=datetime.datetime.now, index=True)
    deletedAt = Column("deleted_at", DateTime)  # 可以为空, 如果非空, 则为软删


@contextlib.contextmanager
def getSession():
    s = Session()
    try:
        yield s
        s.commit()
    except Exception as e:
        s.rollback()
        raise e
    finally:
        s.close()


def createTable():
    Base.metadata.create_all(engine)


def dropTable():
    Base.metadata.drop_all(engine)


def toColumns(table):
    cols = {}
    values = table.__table__.columns.values()
    for v in values:
        cols[v.comment] = v.key
    return cols


def toColumnsAll(table):
    return toColumns(table).update(
        {'id': 'id', 'created_at': 'created_at', 'updated_at': 'updated_at', 'deleted_at': 'deleted_at'})


def toBtColumnsAll(table):
    return toColumns(table).update(
        {
            'opening': 'open',
            'highest': 'high',
            'lowest': 'low',
            'closing': 'close',
            'trading_volume': 'volume',
            'date': 'datetime',
            'id': 'id', 'created_at': 'created_at', 'updated_at': 'updated_at', 'deleted_at': 'deleted_at'
        })


def key2list(table):
    # 将对象的属性名转为list
    values = table.__table__.columns.values()
    keys = [v.key for v in values]
    return keys


# 配合to_dict一起使用
def toJson(all_vendors):  # 多条结果时转为list(json)
    v = [ven.to_dict() for ven in all_vendors]
    return v
