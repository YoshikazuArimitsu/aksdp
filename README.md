# DataProcessing Pipeline template

## 概要

Python でデータ処理パイプラインを開発する為のテンプレート

## 構成

<!-- DIRSTRUCTURE_START_MARKER -->
<pre>
workspace/
├─ README.md ......................... 
├─ requirements_dev.txt .............. 
├─ examples/ ......................... 
│  ├─ titanic.csv .................... 
│  ├─ 1_basic_data_process/ .......... 
│  │  └─ main.py ..................... 
│  ├─ 2_graph/ ....................... 
│  │  └─ main.py ..................... 
│  ├─ 3_graph_branch_merge/ .......... 
│  │  └─ main.py ..................... 
│  └─ 4_sqlalchemy_model_sequential/ . 
│     └─ main.py ..................... 
├─ src/ .............................. 
│  ├─ data/ .......................... 
│  │  ├─ data.py ..................... 
│  │  ├─ dataframe_data.py ........... 
│  │  ├─ raw_data.py ................. 
│  │  └─ sqlalchemy_model_data.py .... 
│  ├─ dataset/ ....................... 
│  │  └─ dataset.py .................. 
│  ├─ graph/ ......................... 
│  │  └─ graph.py .................... 
│  ├─ repository/ .................... 
│  │  ├─ localfile_repository.py ..... 
│  │  ├─ pnadas_db_repository.py ..... 
│  │  ├─ repository.py ............... 
│  │  ├─ s3file_repository.py ........ 
│  │  └─ sqlalchemy_repository.py .... 
│  └─ task/ .......................... 
│     └─ task.py ..................... 
└─ tests/ ............................ 
   ├─ test_localfile_repository.py ... 
   ├─ test_pandas_db_repository.py ... 
   ├─ test_s3file_repository.py ...... 
   ├─ test_sqlalchemy_repository.py .. 
   └─ titanic.csv .................... 
</pre>
<!-- DIRSTRUCTURE_END_MARKER -->


## 目的

* 分析スクリプトがデータ(RDB・ストレージ等)の在処を意識せず、データの変換のみに集中できる枠組みを提供する
* データアクセス層を抽象化し、単体テストを容易にする。
* S3・ローカルのファイルを透過的に扱えるようにする
* DataFrameに対応する
* SQLAlchemyのモデルクラスベースのデータ保存・読み出しに対応する

## 使い方

### データの持ち方

#### Data, Repository

Data は１つのデータ・Repository はそのデータがどこに永続化されるかを指す。
Data/Repository の組み合わせによって保存・読み込み時の動きが異なる。

|Data実装|内部データ|
|:--|:--|
|RawData|バイト列(bytes)|
|DataFrameData|DataFrame|
|SqlAlchemyModelData|SqlAlchemy のモデルのリスト(DataFrameとの相互変換可)|


|Repository実装|保存先|組み合わせ可能なData実装|
|:--|:--|:--|
|LocalFileRepository|ローカルのファイル|RawData, DataFrameData(CSVとして読み出し/保存)|
|S3FileRepository|S3上のファイル|RawData, DataFrameData(CSVとして読み出し/保存)|
|PandasDbRepository|指定DBの指定テーブル|DataFrameData(pandas.to_sql/read_table_sqlにより、1DataFrame=1テーブルのように扱う)|
|SqlAlchemyRepository|指定DB|SqlAlchemyModelData|

##### 使用例

基本 Data と Repository をセットで使用します。

```python
# ローカルのファイルシステムから titanic.csv を DataFrame に読み込む
repo = LocalFileRepository(
   Path(os.path.dirname(__file__)) / Path('titanic.csv'))
titanic_data = DataFrameData.load(repo)
```

Data.content で内部データにアクセスできます

```
df = titanic_data.content
df.head
```

Data.save() を呼び出すと保存します。保存先は Repository により判別します。


#### DataSet

単なる文字列をキーにした Data の dict。

### データの処理

#### Task

DataSet を入力し、DataSet を出力するデータ処理の最小単位。  
Taskクラスを継承して main() にデータの処理を実装する。

#### Graph

Graph を作成し、複数の Task を投入して依存関係を設定する事で実行可能な Task から順に実行する。  

依存先が無い Task には Graph.run() に渡した DataSet が入力として使用される。  
他の Task に依存している Task には依存Taskが出力として返した DataSet をマージした DataSet が入力として使用される。

### SQLAlchemyモデルの使用

#### モデルのクエリ

SqlAlchemyModelData.query(query_func) でDBから指定条件のレコードを抽出します。  
モデルを直接使用する場合は普通の SQLAlchemy と同じです。

#### モデル/DataFrameの相互変換

SqlAlchemyModelData.to_dataframe() で読み込み済みのモデルを DataFrame に変換して取得できます。  

また、SqlAlchemyModelData.update_dataframe() に DataFrame を渡すと DataFrameの内容でモデルを更新します。
DataFrame の行インデックスを参照し、モデルのリストに同番のモデルがあればそのモデルを更新、無ければ追加、削除等により行が抜けている場合はそのモデルを削除します。

## 備考

このテンプレートは全て [VisualStudio Codespaces](https://online.visualstudio.com/)上で開発されています。  

