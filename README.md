## 概要
AcroMUSASHI Stream は、[Storm](http://storm-project.net/) をベースとした、ストリームデータの分散処理プラットフォームです。  
  
「ストリームデータ」とは、ビッグデータにおけるひとつのかたちであり、連続的に発生し続ける時系列順のデータのことを言います。AcroMUSASHI Stream を利用することで、多種多様なデバイス／センサー／サービスなどで発生するストリームデータをリアルタイムに処理するシステムを簡単に構築できるようになります。  
HTTP／SNMP／JMSといった数十種類のプロトコルに対応したインタフェースや、ビッグデータ処理に欠かせない[Hadoop](http://hadoop.apache.org/)／[HBase](http://hbase.apache.org/)／[Cassandra](http://cassandra.apache.org/)などのデータストアとの連携機能を提供しており、「M2M」「ログ収集・分析」「SNSアクセス解析」等、データの解析にリアルタイム性を要するシステムを、迅速に立ち上げることが可能です。  

AcroMUSASHI Stream を用いた実装の仕方については <a href="https://github.com/acromusashi/acromusashi-stream-example"> Examples</a> を参照してください。  

## システム構成イメージ
![Abstract Image](http://acromusashi.github.com/acromusashi-stream/images/AcroMUSASHIStreamAbstract.jpg)

## スタートガイド
### 開発
acromusashi-stream を用いて開発を行うためには、Mavenのビルド定義ファイルであるpom.xmlに以下の内容を記述してください。
```xml
<dependency>  
  <groupId>jp.co.acroquest.acromusashi</groupId>  
  <artifactId>acromusashi-stream</artifactId>  
  <version>0.5.0</version>  
</dependency> 
```
acromusashi-stream を利用して、StormのTopologyを開発してください。

### デプロイ
acromusashi-stream を利用したシステムを実行するためには、以下の手順が必要です。
- Step1: Storm／必要となるミドルウェアのインストール
- Step2: acromusashi-stream を利用して開発したTopologyのデプロイ
- Step3: Topologyの起動

#### Step1: Stormのインストール
[storm-installer](https://github.com/acromusashi/storm-installer)を使用することで、簡単にStormに必要な環境を構築できます。  
また、必要に応じて、メッセージキューやNoSQLなどのミドルウェアをインストールしてください。

#### Step2: acromusashi-stream を利用して開発したTopologyのデプロイ
acromusashi-stream を用いて開発したTopologyのクラスをjarファイルにまとめ、関連するjarファイルと共に、Supervisorにデプロイしてください（関連するjarファイルはSupervisorが動作しているホスト全てにデプロイが必要です）。  
その際、下記のディレクトリに配置してください。
```
関連するjarファイル　 ＞　/opt/storm/lib 配下
開発したTopologyのjar ＞　/opt/storm 配下
```
#### Step3: Topologyの起動
予め、StormのNumbus／Supervisorを起動しておいてください。
```
# service storm-nimbus start
# service storm-supervisor start
```

Storm本体の起動が確認できたら、開発したTopologyを起動します。
```
# cd /opt/storm
# bin/storm jar mytopology-x.x.x-jar acromusashi.stream.example.MyTopology MyTopologyName
```

## 機能一覧

### データ受信／収集
ストリームデータを処理するシステムを構築する際にはデータを受信／取得し、ストリーム処理システムに取り込むこと必要があります。
acromusashi-stream では、以下のようなデータに対応しています（ここでは、代表的な一部だけを示しています）。  
- HTTP（JSON）
- TCP/IP
- SNMP Trap
- Syslog

#### SNMP Trap
SNMP Trap を受信し、Kestrelなどのキューに格納する処理を、設定だけで実現可能です。  
利用方法については、[SNMP Trap 受信機能の利用方法]を確認してください。  

### データ取得
ストリームデータを処理するシステムを構築する際にはデータを一時メッセージキューに格納することで瞬間的な負荷増大に対しても、欠損なく対応できるようになります。  
そのため、ストリームデータ処理プラットフォームではメッセージキューからデータを取得するための機能が求められます。

acromusashi-stream では、以下のメッセージキューに対応しています。  
- [Kestrel](http://robey.github.io/kestrel/)
- [RabbitMQ](http://www.rabbitmq.com/)

#### [Kestrel](http://robey.github.io/kestrel/)
Kestrelからデータを取得するためには、[KestrelJsonSpout](./src/main/java/acromusashi/stream/component/kestrel/spout/KestrelJsonSpout.java)を利用します。  
KestrelからJSON形式のメッセージを取得し、Boltに送信するまでの処理を、シームレスに行えるようになります。  
また、KestrelJsonSpoutを用いた場合、Boltにおいて処理に失敗／タイムアウトしたメッセージの再処理が可能です。  
あらかじめKestrelをインストールしておく必要がありますので、[Kestrelの利用方法](https://github.com/acromusashi/acromusashi-stream-example/wiki/Kestrel-Usage)を確認してインストールして使用してください。

KestrelJsonSpoutを使用する際にはコンストラクタの引数に以下の設定項目を設定してください。  
```
- 第1引数(Kestrelの接続先情報リスト)   
  以下の例のように【Kestrelホスト:KestrelThriftポート】形式のStringのListを指定
  (例)
  - 192.168.0.2:2229  
  - 192.168.0.2:2229  
  - 192.168.0.3:2229  
- 第2引数(Kestrelのメッセージキューベース名称)   
  以下の例のように取得対象となるKestrel上のキュー名のベースの名称を指定してください。  
  (例)MessageQueue  
  尚、ベース名称をMessageQueueとした場合、実際に取得対象となるキュー名は  
  MessageQueue_0、MessageQueue_1、MessageQueue_2・・・となります。  
- 第3引数(Kestrelから取得したデータのデシリアライズ方式)   
  文字列として取得する場合、「new StringScheme())」を設定してください。  
```

##### 実装例
```java
// Kestrelの接続先情報リスト  
List<String> kestrelHosts = Lists.newArrayList("KestrelServer1:2229", "KestrelServer2:2229", "KestrelServer3:2229");  
// Kestrelのメッセージキューベース名称  
String kestrelQueueName = "MessageQueue";  
// KestrelJsonSpoutの並列度  
int kestrelPara = 3;  
  
// KestrelJsonSpoutコンポーネントの生成  
KestrelJsonSpout kestrelSpout = new KestrelJsonSpout(kestrelHosts, kestrelQueueName, new StringScheme());  
// KestrelJsonSpoutをTopologyに登録  
getBuilder().setSpout("KestrelJsonSpout", kestrelSpout, kestrelSpoutPara);  
  
// ～～以後、BoltをTopologyに設定～～  
```

#### [RabbitMQ](http://www.rabbitmq.com/)
RabbitMQからデータを取得するためには、[RabbitMqSpout](./src/main/java/acromusashi/stream/component/rabbitmq/spout/RabbitMqSpout.java)を利用します。  
RabbitMQから文字列形式のメッセージを取得し、グルーピング情報を抽出してBoltに送信するまでの処理を、シームレスに行えるようになります。  
あらかじめRabbitMQをインストールしておく必要がありますので、[RabbitMQの利用方法](https://github.com/acromusashi/acromusashi-stream-example/wiki/RabbitMQ-Usage)を確認してインストールして使用してください。

RabbitMqSpoutにはTopology生成時に以下の設定項目を設定してください。  
その上で、[rabbitmqClusterContext.xml](./conf/rabbitmqClusterContext.xml)を取得して設定を行い、Supervisorが動作しているホスト全ての /opt/storm/conf 配下に配置してください。  
MessageKeyExtractorは[JsonExtractor](https://github.com/acromusashi/acromusashi-stream-example/blob/master/src/main/java/acromusashi/stream/example/spout/JsonExtractor.java)を参考に作成してください。
```
- コンテキスト読込オブジェクト(ContextHelper)  
  RabbitMQのクラスタ設定を記述したコンテキストファイルを読み込む読込オブジェクト。  
  SpringContextHelperに以下の例のようにクラスパス上のコンテキストファイルのパスを設定してください。  
  (例)/rabbitmqClusterContext.xml  
- RabbitMQのベースキュー名称(QueueName)  
  以下の例のように取得対象となるRabbitMQ上のキュー名のベースの名称を指定してください。  
  (例)MessageQueue  
  尚、ベース名称をMessageQueueとした場合、実際に取得対象となるキュー名は  
  MessageQueue0、MessageQueue1、MessageQueue2・・・となります。  
- RabbitMQから取得したメッセージからキーを抽出するオブジェクト(MessageKeyExtractor)
  RabbitMQから取得したメッセージからキー項目を抽出するオブジェクトをMessageKeyExtractorインタフェースを継承して作成し、設定してください。
```

##### 実装例
```java
// RabbitMQクラスタ設定ファイルパスの指定  
String contextPath = "/rabbitmqClusterContext.xml";  
// RabbitMQのメッセージキューベース名称  
String baseQueueName = "MessageQueue";  
// RabbitMqSpoutの並列度  
int mqSpoutPara = 3;  
// MessageKeyExtractorの設定(IPアドレスを抽出するExtractorを設定)  
MessageKeyExtractor extractor = new IpAddressExtractor();  
  
// Springのコンテキスト情報を定義するHelperオブジェクトの生成  
SpringContextHelper helper = new SpringContextHelper(contextPath);  
  
// RabbitMqSpoutコンポーネントの生成  
RabbitMqSpout rabbitMqSpout = new RabbitMqSpout();  
rabbitMqSpout.setContextHelper(helper);  
rabbitMqSpout.setQueueName(baseQueueName);  
rabbitMqSpout.setMessageKeyExtractor(extractor);  

// RabbitMqSpoutをTopologyに登録  
getBuilder().setSpout("RabbitMqSpout", rabbitMqSpout, mqSpoutPara);  
  
// ～～以後、BoltをTopologyに設定～～  
```

##### 設定ファイル記述例([rabbitmqClusterContext.xml](https://github.com/acromusashi/acromusashi-stream/blob/master/conf/rabbitmqClusterContext.xml) をベースに下記の個所の修正を行う)
```xml
<!-- RabbitMQCluster0が保持するキュー一覧 -->  
<util:list id="queueList0">  
    <value>Message0</value>  <!-- ★RabbitMQが保持するキューを設定★ -->  
    <value>Message1</value>  <!-- ★RabbitMQが保持するキューを設定★ -->  
</util:list>  
～～～～  
<!-- RabbitMQプロセス一覧 -->  
<property name="mqProcessList">  
    <util:list list-class="java.util.LinkedList">  
        <value>rabbitmqserver:5672</value>  <!-- ★RabbitMQの待ち受けホスト／ポートを設定★ -->  
    </util:list>  
</property>  
～～～～  
<!-- 呼出元別、接続先RabbitMQプロセス定義 -->  
<property name="connectionProcessMap">  
    <util:map>  
        <entry key="rabbitmqserver_Message0">  <!-- ★RabbitMQへのアクセス元ホスト_キュー名称 を設定★ -->  
            <value>rabbitmqserver:5672</value>  <!-- ★RabbitMQの待ち受けホスト／ポートを設定★ -->  
        </entry>  
        <entry key="rabbitmqserver_Message1">  <!-- ★RabbitMQへのアクセス元ホスト_キュー名称 を設定★ -->  
            <value>rabbitmqserver:5672</value>  <!-- ★RabbitMQの待ち受けホスト／ポートを設定★ -->  
        </entry>  
    </util:map>  
</property>  
～～～～  
<!-- 使用するConnectionFactory （ユーザ名、パスワードを変更） -->  
<bean id="connectionFactory0" class="acromusashi.stream.component.rabbitmq.CachingConnectionFactory">  
    <property name="username" value="guest" />  <!-- ★RabbitMQのユーザ名を設定★ -->  
    <property name="password" value="guest" />  <!-- ★RabbitMQのパスワードを設定★ -->  
    <property name="channelCacheSize" value="10" />  
</bean>  
```

### データストア連携

#### Hadoop
Hadoopに対してデータを投入するためには[HdfsStoreBolt](./src/main/java/acromusashi/stream/bolt/hdfs/HdfsStoreBolt.java)を使用します。  
HDFSに対して一定時間ごとにファイルを切り替えながらデータを投入できるようになります。  
実装例は[Hadoop連携]を確認してください。

HdfsStoreBoltを使用するTopologyでは読み込むYAMLファイルに以下の設定項目を設定してください。
```yaml
## 投入先のHDFSパス  
hdfsstorebolt.outputuri      : 'hdfs://__NAMENODE_HOST__/HDFS/'  
## 投入されるファイル名のヘッダ  
hdfsstorebolt.filenameheader : HDFSStoreBolt  
## ファイルを切り替えるインターバル  
hdfsstorebolt.interval       : 10  
```

#### HBase
HBaseに対してデータを投入するためには[CamelHbaseStoreBolt](./src/main/java/acromusashi/stream/bolt/hbase/CamelHbaseStoreBolt.java)を使用します。  
HBaseに対してBoltが受信したデータを投入できるようになります。  
実装例は[HBase連携]を確認してください。

CamelHbaseStoreBoltには以下の設定項目を設定してください。
```
- コンテキスト設定ファイルパス：クラスパス上に配置したコンテキスト設定ファイルパス  
- HBaseセル定義：CellDefineクラス(ColumnFamilyとColumnQuantifierを保持するクラス）のリスト
```

#### Cassandra
Cassandraに対してデータを投入するためには[CassandraStoreBolt](./src/main/java/acromusashi/stream/component/cassandra/bolt/CassandraStoreBolt.java)を使用します。  
Cassandraに対してBoltが受信したデータを投入できるようになります。  
TupleMapperオブジェクトを切り替えることで投入対象のKeyspace、ColumunFamily、投入内容を切り替えることが可能です。  
##### 実装例
```java
// ～～SpoutをTopologyに設定～～  

// Cassandra投入設定Mapを保持するキー名称  
String configKey = "cassandrastore.setting";  
// Cassandra投入先Keyspace  
String keyspace = "keyspace";  
// Cassandra投入先ColumunFamily  
String columnFamily = "columnFamily";  
// CassandraStoreBoltの並列度  
int cassandraPara = 3;  

// TupleMapperの生成(投入用のデータを生成するMapperオブジェクト)  
TupleMapper<String, String, String> storeMapper = new StoreMapper(keyspace, columnFamily);

// CassandraStoreBoltコンポーネントの生成
CassandraStoreBolt<String, String, String> cassandraStoreBolt = new CassandraStoreBolt<String, String, String>(configKey, storeMapper);
// CassandraStoreBoltをTopologyに登録  
getBuilder().setBolt("CassandraStoreBolt", cassandraStoreBolt, cassandraPara).fieldsGrouping("JsonConvertBolt", new Fields(FieldName.MESSAGE_KEY));  
```
##### 設定ファイル記述例(Topology起動時に読みこませるYAMLファイル)
```yaml
## CassandraStoreBolt Setting  
cassandrastore.setting  :  ## Cassandra設定グループを示すキー項目  
  cassandra.host        : "cassandrahost1:9160,cassandrahost2:9160,cassandrahost3:9160"  ## CassandraHost Setting
  cassandra.clusterName : 'Test Cluster'                                                 ## Cassandra投入先クラスタ名  
  cassandra.connection.timeout : 5000                                                    ## Cassandra接続タイムアウト  
  cassandra.keyspace    :                                                                ## CassandraKeyspace  
    - keyspace  
```

#### Elasticsearch
Elasticsearchに対してデータを投入するためには[ElasticSearchBolt](./src/main/java/acromusashi/stream/component/elasticsearch/bolt/ElasticSearchBolt.java)を使用します。  
Elasticsearchに対してBoltが受信したデータを投入できるようになります。  
実装例は[Elasticsearch連携]を確認してください。

ElasticSearchBoltには以下の設定項目を設定してください。
```
- Elasticsearchクラスタ名：投入先のElasticsearchクラスタ名
- Elasticsearch接続文字列：投入先のElasticsearchクラスタアドレス。「host1:port1;host2:port2;host3:port3...」という形式で定義(ホスト毎の区切り文字はセミコロン)
- Elasticsearch投入先インデックス名称：投入先のElasticsearchインデックス名
- Elasticsearch投入先型名称：投入先のElasticsearch型名
- Elasticsearchに投入するTuple内のフィールド名称：Elasticsearchに投入するTuple中のフィールド名称
```

### ユーティリティ

#### Storm設定読込ユーティリティ
Stormで使用しているYAML形式の設定ファイルを読み込むには[StormConfigGenerator](./src/main/java/acromusashi/stream/config/StormConfigGenerator.java)を使用します。  
YAML形式の設定ファイルをStormのConfigオブジェクトとして読み込むことができます。  
StormのConfigオブジェクトとして読みこんだオブジェクトからは[StormConfigUtil](./src/main/java/acromusashi/stream/config/StormConfigUtil.java)を用いることで値の取得が可能です。

##### 実装例
```java
// 指定したパスから設定情報をStormの設定オブジェクト形式で読み込む。
Config conf = StormConfigGenerator.loadStormConfig("/opt/storm/config/TargetTopology.yaml");

// Stormの設定オブジェクトからキー"target.config"を持つ文字列形式の設定項目をデフォルト値""で取得する
String configValue = StormConfigUtil.getStringValue(conf, "target.config", "");
```

## Javadoc
[Javadoc](http://acromusashi.github.io/acromusashi-stream/javadoc-0.5.0/)

## ダウンロード
https://github.com/acromusashi/acromusashi-stream/wiki/Download

## ライセンス
This software is released under the [MIT License](http://choosealicense.com/licenses/mit/), see LICENSE.txt.
