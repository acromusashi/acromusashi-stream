## 概要
AcroMUSASHI Stream は、Stormをベースとした、ビッグデータの分散ストリームデータ処理プラットフォームです。  

「ストリームデータ」とは、連続的に発生し続ける時系列順のデータのことを言います。AcroMUSASHI Stream を利用することで、多種多様なデバイス／サービスで発生するストリームデータをリアルタイムに処理するシステムを簡単に構築できるようになります。  
HTTP／SNMP／JMSといった数十種類のプロトコルに対応したインタフェースや、ビッグデータ処理に欠かせないHDFS／HBase／Cassandraなどのデータストアとの連携機能を提供しており、「M2M」「ログ収集・分析」「SNSアクセス解析」等、データの解析にリアルタイム性を要するシステムを、迅速に立ち上げることが可能です。  
AcroMUSASHI Streamを用いた実装例については<a href="https://github.com/acromusashi/acromusashi-stream-example">AcroMUSASHI Stream Example</a>を参照してください。  
## システム構成イメージ
![Abstract Image](http://acromusashi.github.com/acromusashi-stream/images/AcroMUSASHIStreamAbstract.jpg)

## スタートガイド
### ビルド環境
* JDK 7以降  
* Maven 2.2.1以降

### ビルド手順
* ソースをGitHubから取得後、取得先ディレクトリに移動し下記のコマンドを実行してください。  
** コマンド実行の結果、 acromusashi-stream.zip が生成されます。  

```
# mvn clean package  
```  

## 機能一覧
### データ取得
#### Kestrel

Kestrelからデータを取得するためには、KestrelJsonSpoutを利用します。  
KestrelからJSON形式のメッセージを取得し、Boltに送信するまでの処理を、シームレスに行えるようになります。  
また、KestrelJsonSpoutを用いた場合、Boltにおいて処理に失敗／タイムアウトしたメッセージの再処理が可能です。  
##### 実装例(BaseTopology継承クラスにおける実装例)
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

### HBase連携

### Cassandra連携

### ElasticSearch連携

### Kestrel連携
KestrelJsonSpoutを用いてKestrelからJSON形式のメッセージを取得し、グルーピング情報を抽出して次Boltに送信する。  
本Spoutを用いた場合、Boltにおいて処理に失敗する／タイムアウトしたメッセージの再処理を行うことが可能。  
##### 実装例(BaseTopology継承クラスにおける実装例)
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

### RabbitMQ連携
RabbitMqSpoutを用いてRabbitMQから文字列形式のメッセージを取得し、グルーピング情報を抽出して次Boltに送信する。  
グルーピング情報の抽出方式はRabbitMqSpoutに設定するMessageKeyExtractor継承クラスによって指定が可能。
#### 実装例(BaseTopology継承クラスにおける実装例)
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
#### 設定ファイル記述例([rabbitmqClusterContext.xml](https://github.com/acromusashi/acromusashi-stream/blob/master/conf/rabbitmqClusterContext.xml) をベースに下記の個所の修正を行う)
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

### SNMPTrap 受信

### DRPC-TridentTopology連携

### Storm設定読込ユーティリティ



## Javadoc
[Javadoc](http://acromusashi.github.io/acromusashi-stream/javadoc-0.5.0/)

## Download
https://github.com/acromusashi/acromusashi-stream/wiki/Download

## Integration

## License
This software is released under the MIT License, see LICENSE.txt.

