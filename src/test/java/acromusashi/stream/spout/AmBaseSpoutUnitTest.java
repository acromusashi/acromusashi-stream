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
package acromusashi.stream.spout;

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

import acromusashi.stream.bolt.ThroughBolt;
import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.trace.KeyHistory;
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
 * AmBaseSpout向けのテストクラス<br>
 * 単一クラスではなく「Stormクラスタ」としての試験のため、AmBaseSpoutTestクラスとは別クラスとして作成する。
 *
 * @author kimura
 */
public class AmBaseSpoutUnitTest
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
     * Spout>Boltのメッセージ送信確認を行う。
     * @throws Exception
     *
     * @target {@link AmBaseSpout#emitWithNoKeyIdAndGrouping(acromusashi.stream.entity.StreamMessage, String)}
     * @test GroupingKey、キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: キー情報履歴にキーを指定し、パラメータを1個指定してメッセージを送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_KeyId未指定_Grouping()
    {
        // 準備
        KeyHistory keyHistory = new KeyHistory();

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        this.mockedSources.addMockData("BlankAmBaseSpout", new Values("GroupingKey", keyHistory,
                message));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new NoKeyIdTestJob());

        // 検証結果確認
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_KeyId未指定_Grouping
     */
    private class NoKeyIdTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        NoKeyIdTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster, createTopology(),
                    AmBaseSpoutUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "ThroughBolt");

            // ThroughBoltでは「messageKey」「keyHistory」「messageValue」フィールドを下流に転送しているため、
            // ThroughBoltが送付するフィールドに値が入っていればAmBaseSpout>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0).toString(), equalTo("GroupingKey"));
            assertThat(resultTuple.get(1), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(1).toString(), equalTo("KeyHistory=[]"));
            assertThat(resultTuple.get(2), instanceOf(StreamMessage.class));
            assertThat(((StreamMessage) resultTuple.get(2)).getField("Param1").toString(),
                    equalTo("Param1"));

            AmBaseSpoutUnitTest.this.isAssert = true;
        }
    }

    /**
     * 下記の構成を保持する検証用Topology定義を作成する。<br>
    * <ol>
    * <li>BlankAmBaseSpout : BlankのAmBaseSpout</li>
    * <li>ThroughBolt : 指定したフィールドを指定してメッセージを下流に流すBolt</li>
    * </ol>
     *
     * @return AmBaseThroughSpout検証用のTopology定義
     */
    private StormTopology createTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();

        // Build Topology
        // Add Spout(BlankAmBaseSpout)
        BlankAmBaseSpout blankSpout = new BlankAmBaseSpout();
        builder.setSpout("BlankAmBaseSpout", blankSpout);

        // Add Bolt(BlankAmBaseSpout -> ThroughBolt)
        ThroughBolt throughBolt = new ThroughBolt();
        throughBolt.setFields(Lists.newArrayList(FieldName.MESSAGE_KEY, FieldName.KEY_HISTORY,
                FieldName.MESSAGE_VALUE));
        builder.setBolt("ThroughBolt", throughBolt).shuffleGrouping("BlankAmBaseSpout");

        StormTopology topology = builder.createTopology();
        return topology;
    }
}
