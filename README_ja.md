# aksdp

## 概要

Python でデータパイプライン処理を記述する為のシンプルなフレームワークです。

## 構成

<!-- DIRSTRUCTURE_START_MARKER -->
<pre>
workspace/
├─ README.md ......................... このファイル
├─ poetry.lock ....................... 
├─ pyproject.toml .................... 
├─ aksdp/ ............................ ソースコード
│  ├─ data/ .......................... 
│  ├─ dataset/ ....................... 
│  ├─ graph/ ......................... 
│  ├─ repository/ .................... 
│  ├─ task/ .......................... 
│  └─ util/ .......................... 
├─ examples/ ......................... 
│  ├─ 1_basic_data_process/ .......... 基本的なデータ処理のサンプル
│  ├─ 2_graph_serial/ ................ Graphを使って複数Task を繋げるサンプル
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
$ pip install git+https://github.com/YoshikazuArimitsu/aksdp.git
```

## QuickStart

```python

class TaskA(Task):
   def main(self, ds:DataSet):
      return ds

class TaskB(Task):
   ...

class TaskC(Task):
   ...

class TaskD(Task):
   ...

graph = Graph()
task_a = graph.append(TaskA())
task_b = graph.append(TaskB(), [task_a])
task_c = graph.append(TaskC(), [task_b])
task_d = graph.append(TaskD(), [task_b, task_c])
graph.run()

```

各タスクはそれぞれの依存タスクが完了後に実行されます。  
また、上流の出力したデータを入力データとして受け取り、処理する事ができます。


### データ

#### DataSet & Data

```python
class UpstreamTask(Task):
   def main(self, ds):
      json_data = JsonData({"key": "value"})
      dataframe_data = DataFrameData(pd.DataFrame([[1, 2, 3], [4, 5, 6]]))

      ds = DataSet()
      ds.put("json_data", json_data)
      ds.put("dataframe_data", json_data)
      return ds

class DownstreamTask(Task):
   def main(self, ds):
      json_data = ds.get("json_data").content
      dataframe_data = ds.get("dataframe_data").content
      ...

```

Data は１つのデータを指します。現状以下の Data実装が存在します。

|Data Class|内部データ|
|:--|:--|
|RawData|バイト列(bytes)|
|DataFrameData|DataFrame|
|JsonData|dict(JSON)|
|SqlAlchemyModelData|SqlAlchemy のモデルのリスト(DataFrameとの相互変換可)|

Data.content で内部データにアクセスできます

```python
df = titanic_data.content
df.head
```

DataSet は任意の数の Data を文字列をキーとして持つクラスです。  
Task間でデータを受け渡しする為に使用します。

#### Repository

Repository は Data の永続化先を示すクラスです。
Data と組み合わせて使用する事により透過的にデータを読み出し・保存する事が可能です。

|Repository実装|保存先|組み合わせ可能なData実装|
|:--|:--|:--|
|LocalFileRepository|ローカルのファイル|RawData, JsonData, DataFrameData(CSVとして読み出し/保存)|
|S3FileRepository|S3上のファイル|RawData, JsonData, DataFrameData(CSVとして読み出し/保存)|
|PandasDbRepository|指定DBの指定テーブル|DataFrameData(pandas.to_sql/read_table_sqlにより、1DataFrame=1テーブルのように扱う)|
|SqlAlchemyRepository|指定DB|SqlAlchemyModelData|
|None|無し|保存時の処理をスキップ|

##### 使用例

```python
# ローカルのファイルシステムから titanic.csv を DataFrame に読み込む
repo = LocalFileRepository(
   Path(os.path.dirname(__file__)) / Path('titanic.csv'))
titanic_data = DataFrameData.load(repo)
```

### データ処理・パイプラインの書き方

#### Task

```python
class xxx(Task):
   def input_datakeys(self) -> tp.List[str]:
      return ['titanic']

   def output_datakeys(self) -> tp.List[str]:
      return ['output_data']

   def main(self, ds:DataSet):
      df = ds.get('titanic').content
      # df = self._in['titanic']  でも参照可能

      ～ 何かの処理 ～

      rds = DataSet()
      rds.put('output_data', ~)
      return rds
```

DataSet を入力し、DataSet を出力するデータ処理の最小単位。  
Taskクラスを継承して main() にデータの処理を実装します。

##### input_datakeys, output_datakeys

DataSet のうち、Taskが使用/出力する Data のキーが分かっている場合は input_datakeys, output_datakeys に定義しておくことができます。  
正しく定義しておくとグラフの依存を自動で組んだり PlantUML の図に注釈が入ります。

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

### examples

#### 1_basic_data_process

CSVから DataFrame にデータを読み込み、Taskを定義して幾つかの変換処理を行うサンプルです。

#### 2_graph_serial

1_basic_data_process と同様の処理を Graph を定義して実行させるサンプルです。

#### 3_graph_branch_merge

Graph を利用し、1つの DataFrame を更新するのではなく各タスクごとに独自の出力を作成し、最後にマージするサンプルです。  
-g オプションで通常のGraph, 並列実行(Thread/Process) を切り替えられます。

#### 4_sqlalchemy_model_sequential

SQLAlchemyのモデルから一行ずつ読み込んで DataSet を作成し、行ごとに処理してDBに書き戻しを行うサンプルです。

#### 5_airflow

Task を AirFlow の DAG に登録するサンプルです。

### その他高度な機能


#### ConcurrentGraph

TaskPoolExecutor/ProcessPoolExecutor により依存関係の無いタスクを並列で実行する Graph です。  
Task側も並列実行される可能性を念頭に実装しないとバグります。使用は自己責任で。

#### エラーハンドラ

```python
def value_error_handler(e, ds):
   print(str(e))
   print(str(ds))

g = Graph()
g.add_error_handler(ValueError, value_error_handler)
```

add_error_handler() で例外発生時に呼び出される関数を追加できます。  
第1引数には発生した例外オブジェクト、第2引数にはエラーを起こした Task の入力DataSetが渡ります。

#### グラフの可視化

```python
print(PlantUML.graph_to_url(graph))
```

util.PlantUML を使用するとタスクの依存関係を PlantUML のグラフ化したURLを生成します。

Task.input_datakeys(), output_datakeys() に Taskが入出力に使用するキーを書いておくと、グラフ化時に可視化されます。

![こんな感じ](http://www.plantuml.com/plantuml/png/ut8eBaaiAYdDpU6A3aWiBWx9ACelJS-8vOfsoyp9yKlqJKt9JCm3SeDJAqBodVDJKe5irzoanABir1IuW6zgKJgGHZ90GLVNJW4ih62bK99PafYNcSo5R2IAWZIWH5vYl6DwAXVS7XG5nQaLyINvySb0SIvKsr6KfKAbu6eTKlDIG7u300=)

#### グラフの自動解決

```python
class 処理A(Task):
   def output_datakeys(self):
      return ["dataA"]
   ...

class 処理B(Task):
   def output_datakeys(self):
      return ["dataB"]
   ...

class 処理C(Task):
   def input_datakeys(self):
      return ["dataA", "dataB"]
   ...


graph = Graph()
taskA = graph.append(処理A())
taskB = graph.append(処理B())
taskC = graph.append(処理C())
graph.autoresolve_dependencies()   # taskC の依存が [taskA, taskB] に設定される
graph.run()
```

Task.input_datakeys(), output_datakeys() に Task が入出力に使用するキーが書かれている場合、
graph.autoresolve_dependencies() を呼び出すと自動で依存関係を解決し、設定します。

Task の input_datakeys() に書かれているキーを output_datakeys() に書いてある Task に依存を設定するもので、
同キーに対しそれを出力する Task が複数見つかった場合失敗します。


#### フック

```python
def pre_run(ds:DataSet):
   with open("ds.pkl", "wb") as f:
      pickle.dump(ds, f)

graph = Graph()

taskA = graph.append(~)
taskA.pre_run_hook = pre_run
taskA.post_run_hook = post_run
```

Graph.append() で追加した GraphTask の pre_run_hook, post_run_hook に関数をセットしておくと、
タスクの開始時、完了時にフック関数をコールバックします。
それぞれ入力・出力の DataSet が渡ってくるのでデバッグ等に利用できます。

#### SQLAlchemyモデルの使用

##### モデルのクエリ

SqlAlchemyModelData.query(query_func) でDBから指定条件のレコードを抽出します。  
モデルを直接使用する場合は普通の SQLAlchemy と同じです。

##### モデル/DataFrameの相互変換

SqlAlchemyModelData.to_dataframe() で読み込み済みのモデルを DataFrame に変換して取得できます。  

また、SqlAlchemyModelData.update_dataframe() に DataFrame を渡すと DataFrameの内容でモデルを更新します。
DataFrame の行インデックスを参照し、モデルのリストに同番のモデルがあればそのモデルを更新、無ければ追加、削除等により行が抜けている場合はそのモデルを削除します。

#### AirFlow化

```
dag = DAG("dag", default_args=default_args, schedule_interval=None)

graph = Graph()
...

af = AirFlow(graph)
af.to_dag(dag)
```

util.Airflow を使用すると各Task を AirFlow向けにラップした PythonOperator を動的に生成し、AirFlow 上で実行できます。

Task の入出力 DataSet は AirFlow の Xcoms により自動で受け渡しされます。

