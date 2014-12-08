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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import acromusashi.stream.bolt.KeyTraceThroughBolt;
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

/**
 * KeyTraceBaseSpout向けのテストクラス<br>
 * 単一クラスではなく「Stormクラスタ」としての試験のため、KeyTraceBaseSpoutTestクラスとは別クラスとして作成する。
 */
public class KeyTraceBaseSpoutUnitTest
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
     * @target {@link KeyTraceBaseSpout#emitWithNoKeyId(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: キー情報履歴にキーを指定し、パラメータを1個指定してメッセージを送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー有()
    {
        // 準備
        KeyHistory keyHistory = new KeyHistory();
        keyHistory.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyHistory,
                "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param1KeyHasTestJob());

        // 検証結果確認
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー有メソッド用のTestJobクラス
     */
    private class Param1KeyHasTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1KeyHasTestJob()
        {}

        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message")),
                    KeyTraceBaseSpoutUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // ThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // ThroughBoltが送付するフィールドに値が入っていればThroughSpout>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue"));

            KeyTraceBaseSpoutUnitTest.this.isAssert = true;
        }
    }

    /**
     * Spout>Boltのメッセージ送信確認を行う。
     * @throws Exception
     *
     * @target {@link KeyTraceBaseSpout#emitWithNoKeyId(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: キー情報履歴にキーを指定せず、パラメータを1個指定してメッセージを送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー無し()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param1NoKeyTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー無しメソッド用のTestJobクラス
     */
    private class Param1NoKeyTestJob implements TestJob
    {

        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1NoKeyTestJob()
        {}

        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message")),
                    KeyTraceBaseSpoutUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // ThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // ThroughBoltが送付するフィールドに値が入っていればThroughSpout>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue"));

            KeyTraceBaseSpoutUnitTest.this.isAssert = true;
        }
    }

    /**
     * Spout>Boltのメッセージ送信確認を行う。
     * @throws Exception
     *
     * @target {@link KeyTraceBaseSpout#emitWithNoKeyId(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: キー情報履歴にキーを指定し、パラメータを3個指定してメッセージを送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー有()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3KeyHasTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ3個_キー有メソッド用のTestJobクラス
     */
    private class Param3KeyHasTestJob implements TestJob
    {

        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3KeyHasTestJob()
        {}

        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3")),
                    KeyTraceBaseSpoutUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // ThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // ThroughBoltが送付するフィールドに値が入っていればThroughSpout>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple.get(3).toString(), equalTo("MessageValue3"));

            KeyTraceBaseSpoutUnitTest.this.isAssert = true;
        }
    }

    /**
     * 装置情報所得処理時の変換確認を行う。
     * @throws Exception
     *
     * @target {@link KeyTraceBaseSpout#emitWithNoKeyId(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: キー情報履歴にキーを指定せず、パラメータを3個指定してメッセージを送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー無し()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3NoKeyTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ3個_キー無しメソッド用のTestJobクラス
     */
    private class Param3NoKeyTestJob implements TestJob
    {

        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3NoKeyTestJob()
        {}

        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3")),
                    KeyTraceBaseSpoutUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // ThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // ThroughBoltが送付するフィールドに値が入っていればThroughSpout>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple.get(3).toString(), equalTo("MessageValue3"));

            KeyTraceBaseSpoutUnitTest.this.isAssert = true;
        }
    }

    /**
     * 下記の構成を保持する検証用Topology定義を作成する。<br>
    * <ol>
    * <li>KeyTraceThroughSpout : 指定したフィールドを指定してグルーピングを定義するSpout</li>
    * <li>KeyTraceThroughBolt : 指定したフィールドを指定してメッセージを送信するBolt</li>
    * </ol>
     *
     * @param contextPath コンテキストパス
     * @return SnmpTrapMessageSendBolt検証用のTopology定義
     */
    private StormTopology createTopology(List<String> fields)
    {
        TopologyBuilder builder = new TopologyBuilder();

        // Build Topology
        // Add Spout(KeyTraceThroughSpout)
        KeyTraceThroughSpout throughSpout = new KeyTraceThroughSpout();
        throughSpout.setFields(fields);
        builder.setSpout("KeyTraceThroughSpout", throughSpout);

        // Add Bolt(KeyTraceThroughSpout -> KeyTraceThroughBolt)
        KeyTraceThroughBolt throughBolt = new KeyTraceThroughBolt();
        throughBolt.setFields(fields);
        builder.setBolt("KeyTraceThroughBolt", throughBolt).shuffleGrouping("KeyTraceThroughSpout");

        StormTopology topology = builder.createTopology();
        return topology;
    }
}
