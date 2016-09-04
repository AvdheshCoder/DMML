from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from schema import Base


class DB:

    def connect(self):
        try:
            engine = create_engine('mysql://root:123@127.0.0.1:3306/dmm')
        except Exception, e:
            print e
        return engine

    def session(self):
        try:
            engine = self.connect()
            Base.metadata.create_all(engine)
            Base.metadata.bind = engine
            session = sessionmaker(bind=engine)()
        except Exception, e:
            print e
        return session

    def add_row(self, row):
        session = self.session()
        try:
            session.add(row)
            session.commit()
        except Exception, e:
            print e
