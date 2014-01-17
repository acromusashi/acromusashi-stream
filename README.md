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

## 機能一覧
### データ受信
ストリームデータを処理するシステムを構築する際にはデータを受信／取得し、ストリーム処理システムに取り込むことが必要になります。  
そのため、ストリームデータ処理基盤にはデータを受信／取得する機能が求められます。  

acromusashi-streamにおいては以下の方式に対応しています。  
- SNMP Trap受信
- Apacheのログ収集
#### SNMP Trap受信

SNMP Trapを受信するにはSNMP Trap受信機能のプロセスを使用します。  
SNMP Trapを受信し、KestrelにJSON形式で保存することができます。  
使用方法は[Camelの利用方法](https://github.com/acromusashi/acromusashi-stream-example/wiki/Camel-Usage)を確認してください。

#### Apacheのログ収集
Apacheのログを収集するには[kafka-log-producer](https://github.com/acromusashi/kafka-log-producer)を使用します。  
詳細は[kafka-log-producer](https://github.com/acromusashi/kafka-log-producer)を確認してください。

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
    <value>Message01</value>  
    <value>Message02</value>  
</util:list>  
～～～～  
<!-- RabbitMQプロセス一覧 -->  
<property name="mqProcessList">  
    <util:list list-class="java.util.LinkedList">  
        <value>rabbitmq01:5672</value>  
        <value>rabbitmq02:5672</value>  
    </util:list>  
</property>  
～～～～  
<!-- 呼出元別、接続先RabbitMQプロセス定義 -->  
<property name="connectionProcessMap">  
    <util:map>  
        <entry key="rabbitmq01_Message01">  
            <value>rabbitmq01:5672</value>  
        </entry>  
        <entry key="rabbitmq01_Message02">  
            <value>rabbitmq02:5672</value>  
        </entry>  
        <entry key="rabbitmq02_Message01">  
            <value>rabbitmq01:5672</value>  
        </entry>  
        <entry key="rabbitmq02_Message02">  
            <value>rabbitmq02:5672</value>  
        </entry>  
    </util:map>  
</property>  
～～～～  
<!-- 使用するConnectionFactory （ユーザ名、パスワードを変更） -->  
<bean id="connectionFactory0" class="acromusashi.stream.component.rabbitmq.CachingConnectionFactory">  
    <property name="username" value="guest" />  
    <property name="password" value="guest" />  
    <property name="channelCacheSize" value="10" />  
</bean>  
```
### データストア連携
#### Hadoop
Hadoopに対してデータを投入するためにはHdfsStoreBoltを使用します。  
HDFSに対して一定時間ごとにファイルを切り替えながらデータを投入できるようになります。  
実装例は[HdfsStoreTopology](https://github.com/acromusashi/acromusashi-stream-example/blob/master/src/main/java/acromusashi/stream/example/topology/HdfsStoreTopology.java)を確認してください。
#### HBase
HBaseに対してデータを投入するためにはCamelHbaseStoreBoltを使用します。  
HBaseに対してBoltが受信したデータを投入できるようになります。  
実装例は[HbaseStoreTopology](https://github.com/acromusashi/acromusashi-stream-example/blob/master/src/main/java/acromusashi/stream/example/topology/HbaseStoreTopology.java)を確認してください。
#### Elasticsearch
Elasticsearchに対してデータを投入するためにはElasticSearchBoltを使用します。  
Elasticsearchに対してBoltが受信したデータを投入できるようになります。  
実装例は[KafkaEsTopology](https://github.com/acromusashi/acromusashi-stream-example/blob/master/src/main/java/acromusashi/stream/example/topology/KafkaEsTopology.java)を確認してください。
#### Cassandra
Cassandraに対してデータを投入するためにはCassandraStoreBoltを使用します。  
Cassandraに対してBoltが受信したデータを投入できるようになります。  
TupleMapperオブジェクトを切り替えることで投入対象のKeyspace、ColumunFamily、投入内容を切り替えることが可能です。  
##### 実装例(BaseTopology継承クラスにおける実装例)
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
### ユーティリティ
#### Storm設定読込ユーティリティ
Stormで使用しているyaml形式の設定ファイルを読み込むにはStormConfigGeneratorを使用します。  
YAML形式の設定ファイルをStormのConfigオブジェクトとして読み込むことができます。  
StormのConfigオブジェクトとして読みこんだオブジェクトからはStormConfigUtilを用いることで値の取得が可能です。  
実装例は[KafkaEsTopology](https://github.com/acromusashi/acromusashi-stream-example/blob/master/src/main/java/acromusashi/stream/example/topology/KafkaEsTopology.java)を確認してください。
## Javadoc
[Javadoc](http://acromusashi.github.io/acromusashi-stream/javadoc-0.5.0/)

## Download
https://github.com/acromusashi/acromusashi-stream/wiki/Download

## License
This software is released under the [MIT License](http://choosealicense.com/licenses/mit/), see LICENSE.txt.

