AcroMUSASHI Streamは分散ストリームプラットフォームです。

AcroMUSASHI Streamは多種多様なデバイス／サービスからのイベントデータをリアルタイムに分散処理します。  
Stormを採用し、ビッグデータに欠かせないHDFS／HBase／Cassandraなどのデータストアとの連携機能を提供します。
![Abstract Image](http://acromusashi.github.com/acromusashi-stream/images/AcroMUSASHIStreamAbstract.jpg)
AcroMUSASHI Streamを用いた実装例については<a href="https://github.com/acromusashi/acromusashi-stream-example">AcroMUSASHI Stream Example</a>を参照してください。


## 機能一覧
### HDFS連携

### HBase連携

### Cassandra連携

### ElasticSearch連携

### Kestrel連携
KestrelJsonSpoutを用いてKestrelからJSON形式のメッセージを取得し、グルーピング情報を抽出して次Boltに送信する。  
本Spoutを用いた場合、Boltにおいて処理に失敗する／タイムアウトしたメッセージの再処理を行うことが可能。  
#### 実装例(BaseTopology継承クラスにおける実装例)
```
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
```
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

### SNMPTrap 受信

### DRPC-TridentTopology連携

### Storm設定読込ユーティリティ

## ビルド手順
### ビルド環境
* JDK 7以降  
* Maven 2.2.1以降

### ビルド手順
* ソースをGitHubから取得後、取得先ディレクトリに移動し下記のコマンドを実行する。  
** コマンド実行の結果、 acromusashi-stream.zip が生成される。  

```
# mvn clean package  
```

## Javadoc
[Javadoc](http://acromusashi.github.io/acromusashi-stream/javadoc-0.5.0/)

## Download
https://github.com/acromusashi/acromusashi-stream/wiki/Download

## Integration

## License
This software is released under the MIT License, see LICENSE.txt.

