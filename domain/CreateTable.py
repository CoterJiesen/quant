from domain.BaseMixin import engine, Base


if __name__ == '__main__':
    # 创建表结构
    Base.metadata.create_all(engine)