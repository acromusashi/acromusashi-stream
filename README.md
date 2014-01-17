## 概要
AcroMUSASHI Stream は、[Storm](http://storm-project.net/)をベースとした、ビッグデータの分散ストリームデータ処理プラットフォームです。  

「ストリームデータ」とは、連続的に発生し続ける時系列順のデータのことを言います。AcroMUSASHI Stream を利用することで、多種多様なデバイス／サービスで発生するストリームデータをリアルタイムに処理するシステムを簡単に構築できるようになります。  
HTTP／SNMP／JMSといった数十種類のプロトコルに対応したインタフェースや、ビッグデータ処理に欠かせない[Hadoop](http://hadoop.apache.org/)／[HBase](http://hbase.apache.org/)／[Cassandra](http://cassandra.apache.org/)などのデータストアとの連携機能を提供しており、「M2M」「ログ収集・分析」「SNSアクセス解析」等、データの解析にリアルタイム性を要するシステムを、迅速に立ち上げることが可能です。  
AcroMUSASHI Streamを用いた実装例については<a href="https://github.com/acromusashi/acromusashi-stream-example">AcroMUSASHI Stream Example</a>を参照してください。  
## システム構成イメージ
![Abstract Image](http://acromusashi.github.com/acromusashi-stream/images/AcroMUSASHIStreamAbstract.jpg)

## スタートガイド
### Maven設定ファイル(pom.xml)の記述内容
acromusashi-streamを用いて開発を行う場合、下記の内容をpom.xmlに追記してください。  
acromusashi-streamのコンポーネントが利用可能になります。  
```xml
<dependency>  
  <groupId>jp.co.acroquest.acromusashi</groupId>  
  <artifactId>acromusashi-stream</artifactId>  
  <version>0.5.0</version>  
</dependency> 
``` 
### インストール
acromusashi-streamを使用するためには以下の手順が必要です。
- 1. Stormのインストール  
- 2. acromusashi-streamのインストール  
- 3. 使用ミドルウェアのインストール  
- 4. Topologyのインストール
- 5. Topologyの起動  

#### 1.Stormのインストール
[storm-installer](https://github.com/acromusashi/storm-installer)を使用します。  
[storm-installer](https://github.com/acromusashi/storm-installer)を参照して下さい。  

#### 2.acromusashi-streamのインストール
- ※Supervisorが動作しているホスト全てにインストールが必要です。  

1.[ダウンロード](https://github.com/acromusashi/acromusashi-stream/blob/master/README.md#%E3%83%80%E3%82%A6%E3%83%B3%E3%83%AD%E3%83%BC%E3%83%89)からacromusashi-streamの媒体をダウンロードします。  
2.Supervisorが動作しているホストにacromusashi-streamの媒体を配置します。    
3.Supervisorが動作しているホストのStormのライブラリディレクトリにファイルをインストールします。  
```
# unzip acromusashi-stream.zip  
# cp -p acromusashi-stream-X.X.X/lib/*.jar /opt/storm/lib/  
``` 
4.Supervisorを再起動します。  
```
# service storm-supervisor restart  
```  
#### 3. 使用ミドルウェアのインストール／4. Topologyのインストール／5. Topologyの起動  
acromusashi-streamの使用したい機能によって異なります。  
[acromusashi-stream-exampleの各機能]を参照してください。  
## 機能一覧
### データ受信
ストリームデータを処理するシステムを構築する際にはデータを受信／取得し、ストリーム処理システムに取り込むことが必要になります。  
そのため、ストリームデータ処理基盤にはデータを受信／取得する機能が求められます。  

acromusashi-streamにおいては以下の方式に対応しています。  
- SNMP Trap受信  

#### SNMP Trap受信

SNMP Trapを受信するにはSNMP Trap受信機能を利用します。  
SNMP Trapを受信し、Kestrelに保存することができます。  
利用方法は[SNMP Trap受信機能の利用方法]を確認してください。  

### データ取得
ストリームデータを処理するシステムを構築する際にはデータを一時メッセージキューに格納することで瞬間的な負荷増大に対しても、欠損なく対応できるようになります。  
そのため、ストリームデータ処理基盤にはメッセージキューからデータを取得するための機能が求められます。

acromusashi-streamにおいては以下のメッセージキューに対応しています。  
- [Kestrel](http://robey.github.io/kestrel/)
- [RabbitMQ](http://www.rabbitmq.com/)

#### [Kestrel](http://robey.github.io/kestrel/)
Kestrelからデータを取得するためには、KestrelJsonSpoutを利用します。  
KestrelからJSON形式のメッセージを取得し、Boltに送信するまでの処理を、シームレスに行えるようになります。  
また、KestrelJsonSpoutを用いた場合、Boltにおいて処理に失敗／タイムアウトしたメッセージの再処理が可能です。  
あらかじめKestrelをインストールしておく必要がありますので、[Kestrelの利用方法](https://github.com/acromusashi/acromusashi-stream-example/wiki/Kestrel-Usage)を確認してインストールして使用してください。

KestrelJsonSpoutには以下の設定項目を設定してください。  
```
- Kestrelの接続先情報リスト：【Kestrelホスト:KestrelThriftポート】形式の文字列のリスト
- Kestrelのメッセージキューベース名称：キュー名称のベースを定義。【ベース名称】_【KestrelJsonSpoutのスレッドID】のキューが取得対象
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
RabbitMQからデータを取得するためにはRabbitMqSpoutを利用します。  
RabbitMQから文字列形式のメッセージを取得し、グルーピング情報を抽出してBoltに送信するまでの処理を、シームレスに行えるようになります。  
あらかじめRabbitMQをインストールしておく必要がありますので、[RabbitMQの利用方法]を確認してインストールして使用してください。

RabbitMqSpoutには以下の設定項目を設定し、rabbitmqClusterContext.xmlにも設定を行ってください。
```
- RabbitMQクラスタ設定ファイルパス：クラスパス上に配置したRabbitMQクラスタ設定ファイルパス
- RabbitMQのメッセージキューベース名称：キュー名称のベースを定義。【ベース名称】【RabbitMqSpoutのスレッドID】のキューが取得対象
- MessageKeyExtractorを継承したキー抽出クラス（個別に実装が必要です）
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
Hadoopに対してデータを投入するためにはHdfsStoreBoltを使用します。  
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
HBaseに対してデータを投入するためにはCamelHbaseStoreBoltを使用します。  
HBaseに対してBoltが受信したデータを投入できるようになります。  
実装例は[HBase連携]を確認してください。

CamelHbaseStoreBoltには以下の設定項目を設定してください。
```
- コンテキスト設定ファイルパス：クラスパス上に配置したコンテキスト設定ファイルパス  
- HBaseセル定義：CellDefineクラス(ColumnFamilyとColumnQuantifierを保持するクラス）のリスト
```
#### Cassandra
Cassandraに対してデータを投入するためにはCassandraStoreBoltを使用します。  
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
Elasticsearchに対してデータを投入するためにはElasticSearchBoltを使用します。  
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
Stormで使用しているyaml形式の設定ファイルを読み込むにはStormConfigGeneratorを使用します。  
YAML形式の設定ファイルをStormのConfigオブジェクトとして読み込むことができます。  
StormのConfigオブジェクトとして読みこんだオブジェクトからはStormConfigUtilを用いることで値の取得が可能です。    
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

## License
This software is released under the [MIT License](http://choosealicense.com/licenses/mit/), see LICENSE.txt.

