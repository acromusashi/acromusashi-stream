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
package acromusashi.stream.bolt;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.spout.BlankAmBaseSpout;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

/**
 * AmBaseBolt向けのテストクラス<br>
 * 単一クラスではなく「Stormクラスタ」としての試験のため、AmBaseBoltTestクラスとは別クラスとして作成する。
 *
 * @author kimura
 */
public class AmBaseBoltUnitTest
{
    /** JUnit実行結果を保持する変数 */
    private boolean               isAssert;

    /** テスト用クラスタパラメータ */
    private static MkClusterParam mkClusterParam;

    /** テスト用Daemonコンフィグ */
    private static Config         daemonConf;

    /** テスト用Topologyコンフィグ */
    Config                        topologyConf;

    /** テスト用データソース */
    MockedSources                 mockedSources;

    /** Topology完了条件 */
    CompleteTopologyParam         completeTopologyParam;

    /**
     * クラス単位の初期化処理を実行する。<br>
     * クラスタ用パラメータとDaemon用パラメータを生成する。
     */
    @BeforeClass
    public static void initialSetup()
    {
        // クラスタ用パラメータとDaemon用パラメータを初期化
        mkClusterParam = new MkClusterParam();
        daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
    }

    /**
     * テストケース単位の初期化処理を実行する。<br>
     */
    @Before
    public void setup()
    {
        // テスト実施結果の格納変数を初期化
        this.isAssert = false;
        // モックデータソースを初期化
        this.mockedSources = new MockedSources();
        // Topology用設定値を初期化
        this.topologyConf = new Config();

        // Topology完了条件を初期化
        this.completeTopologyParam = new CompleteTopologyParam();
        this.completeTopologyParam.setMockedSources(this.mockedSources);
        this.completeTopologyParam.setStormConf(this.topologyConf);
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emit(StreamMessage, Object)}
     * @test メッセージキー、メッセージ値が次のBoltに指定したキーで配信されること
     *    condition:: キー指定、キー重複なしの状態で1メッセージ送信する
     *    result:: メッセージキー、メッセージ値が次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_メッセージ送信確認()
    {
        // 準備
        StreamMessage message = new StreamMessage();
        message.addField(FieldName.MESSAGE_KEY, "StreamMessageKey");
        message.addField(FieldName.MESSAGE_VALUE, "StreamMessageValue");
        message.getHeader().addHistory("BeforeMessageKey");

        this.mockedSources.addMockData("BlankAmBaseSpout", new Values("GroupingKey", message));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new MessageConfirmTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_メッセージ送信確認メソッド用のTestJobクラス
     */
    private class MessageConfirmTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        MessageConfirmTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster, createTopology(),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "ThroughBolt");

            // ThroughBoltでは「messageKey」「messageValue」フィールドを下流に送付しているため、
            // ThroughBoltが送付するフィールドに値が入っていればAmBaseThroughBolt>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0).toString(), equalTo(""));

            assertThat(resultTuple.get(1), instanceOf(StreamMessage.class));
            StreamMessage sendMessage = (StreamMessage) resultTuple.get(1);

            assertThat(sendMessage.getHeader().getHistory().toString(),
                    equalTo("KeyHistory=[BeforeMessageKey, KeyHistory]"));
            assertThat(sendMessage.getField(FieldName.MESSAGE_KEY).toString(),
                    equalTo("GroupingKey"));
            assertThat(sendMessage.getField(FieldName.MESSAGE_VALUE).toString(),
                    equalTo("StreamMessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * 下記の構成を保持する検証用Topology定義を作成する。<br>
     * <ol>
     * <li>BlankAmBaseSpout : 指定したフィールドを指定してグルーピングを定義するSpout</li>
     * <li>AmBaseThroughBolt : 指定したフィールドを指定し、キーを追加してメッセージを送信するBolt</li>
     * <li>ThroughBolt : 指定したフィールドを指定してメッセージを送信するBolt</li>
     * </ol>
     *
     * @return 該当のTopology定義
     */
    private StormTopology createTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();

        // Build Topology
        // Add Spout(BlankAmBaseSpout)
        BlankAmBaseSpout blankAmBaseSpout = new BlankAmBaseSpout();
        builder.setSpout("BlankAmBaseSpout", blankAmBaseSpout);

        // Add Bolt(BlankAmBaseSpout -> AmBaseThroughBolt)
        AmBaseThroughBolt amBaseThroughBolt = new AmBaseThroughBolt();
        amBaseThroughBolt.setFields(Lists.newArrayList(FieldName.MESSAGE_KEY,
                FieldName.MESSAGE_VALUE));
        builder.setBolt("AmBaseThroughBolt", amBaseThroughBolt).shuffleGrouping("BlankAmBaseSpout");

        // Add Bolt(AmBaseThroughBolt -> ThroughBolt)
        ThroughBolt throughBolt = new ThroughBolt();
        throughBolt.setFields(Lists.newArrayList(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
        builder.setBolt("ThroughBolt", throughBolt).shuffleGrouping("AmBaseThroughBolt");

        StormTopology topology = builder.createTopology();
        return topology;
    }
}
