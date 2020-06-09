from sqlalchemy.orm import scoped_session, sessionmaker
from logging import getLogger
from aksdp.data import Data, DataType
from .repository import Repository

logger = getLogger(__name__)


class SqlAlchemyRepository(Repository):
    Session = None

    def __init__(self, engine):
        session_factory = sessionmaker(bind=engine)
        Session = scoped_session(session_factory)
        self.session = Session()

    def __del__(self):
        """.dtor
        セッションのクローズ
        """
        if self.session:
            self.session.close()

    def save(self, data: Data):
        if data.data_type == DataType.SQLALCHEMY_MODEL:
            self.commit()
        else:
            raise ValueError(f"SqlAlchemyRepository.save() not support DataType {data.data_type}")

    def query(self, model_class):
        return self.session.query(model_class)

    def insert(self, model):
        self.session.add(model)
        return model

    def delete(self, model):
        self.session.delete(model)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
