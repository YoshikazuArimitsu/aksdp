# DataProcessing Pipeline template

## 概要

Python でデータ処理パイプラインを開発する為のテンプレート

## 構成

<!-- DIRSTRUCTURE_START_MARKER -->
<pre>
workspace/
├─ README.md ......................... README
├─ poetry.lock ....................... 
├─ pyproject.toml .................... 
├─ aksdp/ ............................ 
│  ├─ data/ .......................... 
│  ├─ dataset/ ....................... 
│  ├─ graph/ ......................... 
│  ├─ repository/ .................... 
│  └─ task/ .......................... 
├─ examples/ ......................... 
│  ├─ 1_basic_data_process/ .......... 基本的なデータ処理のサンプル
│  ├─ 2_graph_serial/ ................ Graphを使って複数Task を繋げるサンプル
│  ├─ 3.5_graph_concurrent/ .......... Graph並列実行のサンプル
│  ├─ 3_graph_branch_merge/ .......... GraphでTaskの依存関係が分岐・合流するサンプル
│  └─ 4_sqlalchemy_model_sequential/ . SQLAlchemy を利用したレコードごとの順次処理を行うサンプル
└─ tests/ ............................ UT
</pre>
<!-- DIRSTRUCTURE_END_MARKER -->


## 目的

* 分析スクリプトがデータ(RDB・ストレージ等)の在処を意識せず、データの変換のみに集中できる枠組みを提供する
* データアクセス層を抽象化し、単体テストを容易にする。
* S3・ローカルのファイルを透過的に扱えるようにする
* DataFrameに対応する
* SQLAlchemyのモデルクラスベースのデータ保存・読み出しに対応する
* 古き良き Python開発環境からの卒業

## 使い方

### インストール

```
$ pip install git+https://github.com/YoshikazuArimitsu/DataProcessingPipiline_template
```

### データの持ち方

#### Data, Repository

Data は１つのデータ・Repository はそのデータがどこに永続化されるかを指す。
Data/Repository の組み合わせによって保存・読み込み時の動きが異なる。

|Data実装|内部データ|
|:--|:--|
|RawData|バイト列(bytes)|
|DataFrameData|DataFrame|
|JsonData|dict(JSON)|
|SqlAlchemyModelData|SqlAlchemy のモデルのリスト(DataFrameとの相互変換可)|


|Repository実装|保存先|組み合わせ可能なData実装|
|:--|:--|:--|
|LocalFileRepository|ローカルのファイル|RawData, JsonData, DataFrameData(CSVとして読み出し/保存)|
|S3FileRepository|S3上のファイル|RawData, JsonData, DataFrameData(CSVとして読み出し/保存)|
|PandasDbRepository|指定DBの指定テーブル|DataFrameData(pandas.to_sql/read_table_sqlにより、1DataFrame=1テーブルのように扱う)|
|SqlAlchemyRepository|指定DB|SqlAlchemyModelData|
|None|無し|保存時の処理をスキップ|

##### 使用例

基本 Data と Repository をセットで使用します。

```python
# ローカルのファイルシステムから titanic.csv を DataFrame に読み込む
repo = LocalFileRepository(
   Path(os.path.dirname(__file__)) / Path('titanic.csv'))
titanic_data = DataFrameData.load(repo)
```

Data.content で内部データにアクセスできます

```python
df = titanic_data.content
df.head
```

Data.save() を呼び出すと保存します。保存先は Repository により判別します。


#### DataSet

```python
ds.put('titanic', titanic_data)
df = ds.get('titanic').content

ds.save_all()
```

単なる文字列をキーにした Data の dict。  
DataSet.put()/get() で所有するデータにアクセス。

DataSet.save_all() を呼び出すと所有するデータを全て保存する。

### データの処理

#### Task

```python
class xxx(Task):
   def main(self, ds:DataSet):
      df = ds.get('titanic').content

      ～ 何かの処理 ～

      return ds
```

DataSet を入力し、DataSet を出力するデータ処理の最小単位。  
Taskクラスを継承して main() にデータの処理を実装する。


#### Graph

Graph を作成し、複数の Task を投入して依存関係を設定する事で実行可能な Task から順に実行する。  

依存先が無い Task には Graph.run() に渡した DataSet が入力として使用される。  
他の Task に依存している Task には依存Taskが出力として返した DataSet をマージした DataSet が入力として使用される。

```python
class 処理A(Task):
   ...

class 処理B(Task):
   ...

class 処理C(Task):
   ...

class 処理D(Task):
   ...

graph = Graph()
taskA = graph.append(処理A())                   # 依存なし。graph.run() に指定した DataSet がINPUT
taskB = graph.append(処理B())                   # 依存なし。graph.run() に指定した DataSet がINPUT
taskC = graph.append(処理C(), [taskA])          # taskAに依存。taskAの出力した DataSet がINPUT
taskD = graph.append(処理D(), [taskB, taskC])   # taskB, taskCに依存。taskB&Cの出力した DataSet をマージしたものがINPUT
graph.run(ds)
```

##### ConcurrentGraph

マルチスレッド対応の Graphサンプルです。使用は自己責任で。

##### エラーハンドラ

```python
def value_error_handler(e, ds):
   print(str(e))
   print(str(ds))

g = Graph()
g.add_error_handler(ValueError, value_error_handler)
```

add_error_handler() で例外発生時に呼び出される関数を追加できます。  
第1引数には発生した例外オブジェクト、第2引数にはエラーを起こした Task の入力DataSetが渡ります。

##### グラフの可視化

```python
print(PlantUML.graph_to_url(graph))
```

util.PlantUML を使用するとタスクの依存関係を PlantUML のグラフ化したURLを生成します。

### SQLAlchemyモデルの使用

#### モデルのクエリ

SqlAlchemyModelData.query(query_func) でDBから指定条件のレコードを抽出します。  
モデルを直接使用する場合は普通の SQLAlchemy と同じです。

#### モデル/DataFrameの相互変換

SqlAlchemyModelData.to_dataframe() で読み込み済みのモデルを DataFrame に変換して取得できます。  

また、SqlAlchemyModelData.update_dataframe() に DataFrame を渡すと DataFrameの内容でモデルを更新します。
DataFrame の行インデックスを参照し、モデルのリストに同番のモデルがあればそのモデルを更新、無ければ追加、削除等により行が抜けている場合はそのモデルを削除します。

### examples

#### 1_basic_data_process

CSVから DataFrame にデータを読み込み、Taskを定義して幾つかの変換処理を行うサンプルです。

#### 2_graph_serial

1_basic_data_process と同様の処理を Graph を定義して実行させるサンプルです。

#### 3_graph_branch_merge

Graph を利用し、1つの DataFrame を更新するのではなく各タスクごとに独自の出力を作成し、最後にマージするサンプルです

##### 3.5_graph_concurrent

3_graph_branch_merge を ConcurrengGraph を使用して実行します。

#### 4_sqlalchemy_model_sequential

SQLAlchemyのモデルから一行ずつ読み込んで DataSet を作成し、行ごとに処理してDBに書き戻しを行うサンプルです。


## 備考

このテンプレートは全て [VisualStudio Codespaces](https://online.visualstudio.com/)上で開発されています。  

