from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from pathlib import Path
import os
import unittest
from logging import getLogger
from aksdp.data import RawData, DataFrameData, SqlAlchemyModelData
from aksdp.repository import SqlAlchemyRepository, LocalFileRepository

logger = getLogger(__name__)
Base = declarative_base()


class Titanic(Base):
    __tablename__ = "titanic"

    PassengerId = Column(Integer, primary_key=True)
    Survived = Column(Integer)
    Pclass = Column(Integer)
    Name = Column(String)
    Sex = Column(String)


class TestSqlAlchemyRepository(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///ut.sqlite3", echo=True)

        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def table_init(self):
        repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("titanic.csv"))
        data = DataFrameData.load(repo)

        repo_s = SqlAlchemyRepository(self.engine)

        md = SqlAlchemyModelData(repo_s, Titanic)
        md.update_dataframe(data.content)
        md.save()

    def test_query_and_update(self):
        self.table_init()

        # SQLAlchemyのクエリでデータを抽出
        repo = SqlAlchemyRepository(self.engine)
        d = SqlAlchemyModelData(repo, Titanic)
        d.query(lambda x: x.filter(Titanic.PassengerId == 1))
        self.assertEqual(1, len(d.content))
        self.assertEqual(1, d.content[0].PassengerId)

        # DataFrameとして取得
        df = d.to_dataframe()
        self.assertEqual("Braund, Mr. Owen Harris", df.at[0, "Name"])

        # DataFrameを更新し、書き戻す
        df.loc[df.index[0], "Name"] = "Taro Yamada"
        d.update_dataframe(df)
        d.save()

        # 再度取得、値が変わっているか確認
        d = SqlAlchemyModelData(repo, Titanic)
        d.query(lambda x: x.filter(Titanic.PassengerId == 1))
        self.assertEqual(1, len(d.content))
        self.assertEqual(1, d.content[0].PassengerId)
        self.assertEqual("Taro Yamada", d.content[0].Name)

    def test_query_and_remove(self):
        self.table_init()

        # SQLAlchemyのクエリでデータを抽出
        repo = SqlAlchemyRepository(self.engine)
        d = SqlAlchemyModelData(repo, Titanic)
        d.query(lambda x: x.filter(Titanic.PassengerId <= 10))

        # DataFrameとして取得
        df = d.to_dataframe()
        self.assertEqual("Braund, Mr. Owen Harris", df.at[0, "Name"])

        # DataFrameの行を削除し、書き戻す
        df = df.drop(df.index[[0, 1, 2]])
        print(df)
        d.update_dataframe(df)
        d.save()

        # PassengerId 1,2,3 のレコードが消えている事の確認
        d = SqlAlchemyModelData(repo, Titanic)
        d.query(lambda x: x.filter(Titanic.PassengerId < 4))
        self.assertEqual(0, len(d.content))
