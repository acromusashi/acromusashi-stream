/**
* Copyright (c) Acroquest Technology Co, Ltd. All Rights Reserved.
* Please read the associated COPYRIGHTS file for more details.
*
* THE SOFTWARE IS PROVIDED BY Acroquest Technolog Co., Ltd.,
* WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
* BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDER BE LIABLE FOR ANY
* CLAIM, DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
* OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
*/
package acromusashi.stream.topology;

import java.util.List;

import acromusashi.stream.bolt.StreamMessagePrintBolt;
import acromusashi.stream.component.kestrel.spout.KestrelJsonSpout;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.MessageEntity;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.entity.StreamMessageHeader;
import backtype.storm.Config;
import backtype.storm.scheme.StringScheme;
import backtype.storm.tuple.Fields;

/**
 * Kestrelからのメッセージ取得用のTopologyを起動する。
 * <br>
 * Topologyの動作フローは下記の通り。<br>
 * <ol>
 * <li>KestrelThriftSpoutにてSNMPメッセージをJSON形式で受信する</li>
 * <li>StreamMessagePrintBoltにて受信したメッセージをログ出力する</li>
 * </ol>
 *
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>kestrel.servers : Kestrelが配置されるホスト:Portの配列(デフォルト値:無)</li>
 * <li>kestrel.queue : Kestrelのキュー名称(デフォルト値:MessageQueue)</li>
 * <li>kestrel.parallelism : KestrelThriftSpoutの並列度(デフォルト値:1)</li>
 * <li>print.parallelism : CamelJdbcStoreBoltの並列度(デフォルト値:1)</li>
 * </ul>
 * @author kimura
 */
public class CommonMessagePrintTopology extends BaseTopology
{
    /**
     * コンストラクタ
     *
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public CommonMessagePrintTopology(String topologyName, Config config)
    {
        super(topologyName, config);
    }

    /**
     * プログラムエントリポイント<br>
     * <ul>
     * <li>起動引数:arg[0] 設定値を記述したyamlファイルパス</li>
     * <li>起動引数:arg[1] Stormの起動モード(true:LocalMode、false:DistributeMode)</li>
     * </ul>
     * @param args 起動引数
     * @throws Exception 初期化例外発生時
     */
    public static void main(String[] args) throws Exception
    {
        // プログラム引数の不足をチェック
        if (args.length < 2)
        {
            System.out.println("Usage: java acromusashi.stream.topology.CommonMessagePrintTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new CommonMessagePrintTopology("CommonMessagePrintTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void buildTopology() throws Exception
    {
        // Get setting from StormConfig Object
        List<String> kestrelHosts = StormConfigUtil.getStringListValue(getConfig(),
                KestrelJsonSpout.KESTREL_SERVERS);
        String kestrelQueueName = StormConfigUtil.getStringValue(getConfig(),
                KestrelJsonSpout.KESTREL_QUEUE, "MessageQueue");
        int kestrelSpoutPara = StormConfigUtil.getIntValue(getConfig(), "kestrel.parallelism", 1);
        int printPara = StormConfigUtil.getIntValue(getConfig(), "print.parallelism", 1);

        // Topology Setting
        // Add Spout(KestrelJsonSpout)
        KestrelJsonSpout kestrelSpout = new KestrelJsonSpout(kestrelHosts, kestrelQueueName,
                new StringScheme());
        getBuilder().setSpout("KestrelJsonSpout", kestrelSpout, kestrelSpoutPara);

        // Add Bolt(KestrelJsonSpout -> StreamMessagePrintBolt)
        StreamMessagePrintBolt streamMessagePrintBolt = new StreamMessagePrintBolt();
        getBuilder().setBolt("StreamMessagePrintBolt", streamMessagePrintBolt, printPara).fieldsGrouping(
                "KestrelJsonSpout", new Fields(FieldName.MESSAGE_KEY));

        // Regist Serialize Setting.
        getConfig().registerSerialization(StreamMessage.class);
        getConfig().registerSerialization(StreamMessageHeader.class);
        getConfig().registerSerialization(MessageEntity.class);
    }
}
