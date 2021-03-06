# aksdp

## 概要

Python でデータパイプライン処理を記述する為のシンプルなフレームワークです。

## 構成

<!-- DIRSTRUCTURE_START_MARKER -->
<pre>
aksdp/
├─ LICENSE ........................... ライセンス情報
├─ README.md ......................... README(en)
├─ README_ja.md ...................... README(jp) このファイル
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
│  ├─ 4_graph_dynamic/ ............... ouput_datakeys() の切り替えによる動的分岐のサンプル
│  ├─ 5_sqlalchemy_model_sequential/ . SQLAlchemyのモデルをデータとして使用するサンプル
│  ├─ 6_airflow/ ..................... task を airflow の DAG に変換するサンプル
│  ├─ 7_debug/ ....................... DebugGraphの使用例
│  └─ 8_graph_from_yaml/ ............. YAMLからGraphを構築するサンプル
└─ tests/ ............................ UT
</pre>
<!-- DIRSTRUCTURE_END_MARKER -->


## 特徴

* データ駆動
* 充実したデバッグ用機能
* 導入が簡単、捨てるのも簡単 (フレームワークの設定ファイル・ディレクトリ構造等にロックインしない)

なデータパイプライン処理フレームワーク。

## 使い方

### インストール

```
$ pip install aksdp
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
   def __init__(self, params:dict):
      super().__init__(params)

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

##### params

タスクの設定・パラメータ等を設定します。  
Task.params プロパティに設定されます。初期化しなかった場合、{} になります。

##### input_datakeys, output_datakeys

DataSet のうち、Taskが使用/出力する Data のキーが分かっている場合は input_datakeys, output_datakeys に定義しておくことができます。  
正しく定義しておくとグラフの依存を自動で組んだり PlantUML の図に注釈が入ります。

#### Graph

Graph を作成し、複数の Task を投入して依存関係を設定する事で実行可能な Task から順に実行されます。
Task への入力DataSetは以下 DataSet を動的にキー単位でマージして作成します。

1. 上流 Task の出力 DataSet
2. Default DataSet  
   上流 Task が存在しない Task には Graph.run() の引数として与えた DataSet が入力に使用されます
3. Catalog DataSet
   Graph のコンストラクタで catalog_ds として指定した DataSet は全ての Task の入力に使用されます

```python
class 処理A(Task):
   ...

class 処理B(Task):
   ...

class 処理C(Task):
   ...

class 処理D(Task):
   ...

graph = Graph(catalog_ds)
taskA = graph.append(処理A())                   # default_ds + catalog_ds がINPUT
taskB = graph.append(処理B())                   # default_ds + catalog_ds がINPUT
taskC = graph.append(処理C(), [taskA])          # taskA の出力DS + catalog_ds がINPUT
taskD = graph.append(処理D(), [taskB, taskC])   # taskB&C の出力DS + catalog_ds がINPUT
graph.run(default_ds)
```

### examples

#### 1_basic_data_process

CSVから DataFrame にデータを読み込み、Taskを定義して幾つかの変換処理を行うサンプルです。

#### 2_graph_serial

1_basic_data_process と同様の処理を Graph を定義して実行させるサンプルです。

#### 3_graph_branch_merge

Graph を利用し、1つの DataFrame を更新するのではなく各タスクごとに独自の出力を作成し、最後にマージするサンプルです。  
-g オプションで通常のGraph, 並列実行(Thread/Process) を切り替えられます。

#### 4_graph_dynamic

Graph の動的解決を使用した上流の処理結果によって下流のタスクを切り替えるサンプルです。

#### 5_sqlalchemy_model_sequential

SQLAlchemyのモデルから一行ずつ読み込んで DataSet を作成し、行ごとに処理してDBに書き戻しを行うサンプルです。

#### 6_airflow

Task を AirFlow の DAG に登録するサンプルです。

### その他高度な機能

#### YAML/JSONからのグラフ構築

```yaml
includes:
   - base.yml

graph:
    class:  Graph

tasks:
  - name: taskA
    class: main.TaskA
    params:
      string_param: string_value
      int_param: 42
  - name: taskB
    class: main.TaskB
    dependencies:
      - taskA
  ...
```

```python
from aksdp.util import graph_factory as gf

graph = gf.create_from_file(Path("graph.yml"))
```

graph_factory.create_from_file で設定ファイルからグラフを構築する事が可能です。

tasks.name はそのタスクに依存するタスクの依存を記述する必要がある時以外は省略可能です。  
tasks.dependencies にはタスクが依存するタスクの name を記述します。  
task.class はクラスのパスです。import は自動で行います。  
task.params はタスクのパラメータです。params下の dict がタスクのインスタンス生成時に渡ります。

includes を使用すると複数の設定ファイルからグラフを構築します。
graph は外側で定義したものが優先、tasks のリストはマージされます。

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
class TaskA(Task):
   def output_datakeys(self):
      return ["dataA"]
   ...

class TaskB(Task):
   def output_datakeys(self):
      return ["dataB"]
   ...

class TaskC(Task):
   def input_datakeys(self):
      return ["dataA", "dataB"]
   ...


graph = Graph()
taskA = graph.append(TaskA())
taskB = graph.append(TaskB())
taskC = graph.append(TaskC())
graph.run()   # A&B -> C の順で実行
```

Task.input_datakeys(), output_datakeys() に Task が入出力に使用するキーが書かれている場合、
依存タスクを明示的に指定せずとも自動で依存関係を解決し、設定します。

#### グラフの動的解決

```python
class TaskA(Task):
   def output_datakeys(self):
      return self._output_datakeys

   def main(self, ds:DataSet):
      rds = DataSet()
      if {条件A}:
         self._output_datakeys = ["DataA-1"]
         rds.put("DataA-1", 〜)
      else:
         self._output_datakeys = ["DataA-2"]
         rds.put("DataA-2", 〜)
      return rds

class TaskB(Task):
   def input_datakeys(self):
      return ["DataA-1"]
   ...

class TaskC(Task):
   def input_datakeys(self):
      return ["DataA-2"]
   ...
```

上記のように output_datakeys() は条件によって切り替える事が可能です。
TaskA の実行完了時に有効になった output_datakeys によって TaskB or TaskC のどちらかが実行されます。

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

#### DebugGraph

```python
graph = DebugGraph(Path('./dump'))
graph.append(TaskA())
task_b = graph.append(TaskB())

if run:
   graph.run()
else:
   graph.run_task(task_b)
```

```bash
$ tree ./dump
./dump/
├── TaskA
│   ├── in
│   │   └── TaskA.pkl
│   └── out
│       ├── TaskA.pkl
│       ├── xxx.csv
...
└── TaskB
    ├── in
    └── out
```

フック機能を利用し、run() の実行時に各タスクの入出力を全て指定ディレクトリ下に保存する Graph です。  
また、run_task(task) を呼び出すと run() 時に生成した pkl から入力データを復元し、ワンショットでタスクを実行してデバッグする事が可能です。


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

