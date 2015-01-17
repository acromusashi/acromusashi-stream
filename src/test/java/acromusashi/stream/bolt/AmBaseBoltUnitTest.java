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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import acromusashi.stream.spout.BlankAmBaseSpout;
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
 * KeyTraceBaseBolt向けのテストクラス<br>
 * 単一クラスではなく「Stormクラスタ」としての試験のため、KeyTraceBaseBoltTestクラスとは別クラスとして作成する。
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
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ1個、キー指定、キー重複なしの状態で1メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー有_重複なし_1メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param1KeyHasUnique1MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー有_重複なし_1メッセージメソッド用のTestJobクラス
     */
    private class Param1KeyHasUnique1MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1KeyHasUnique1MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message"), Arrays.asList("ChildKey")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey, ChildKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ1個、キー指定、キー重複なしの状態で1メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー有_重複なし_1メッセージ_Ack()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam,
                new Param1KeyHasUnique1MsgAckTestJob());

        // 検証結果確認
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー有_重複なし_1メッセージ_Ack用のTestJobクラス
     */
    private class Param1KeyHasUnique1MsgAckTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1KeyHasUnique1MsgAckTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createAckTopology(Arrays.asList("Message"), Arrays.asList("ChildKey")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey, ChildKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ1個、キー指定、キー重複なしの状態で3メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー有_重複なし_3メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param1KeyHasUnique3MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー有_重複なし_3メッセージメソッド用のTestJobクラス
     */
    private class Param1KeyHasUnique3MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1KeyHasUnique3MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(
                    cluster,
                    createTopology(Arrays.asList("Message"),
                            Arrays.asList("ChildKey1", "ChildKey2", "ChildKey3")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 3);

            // 1メッセージ目検証
            List resultTuple1 = (List) resultList.get(0);

            assertThat(resultTuple1.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple1.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey1]"));
            assertThat(resultTuple1.get(1).toString(), equalTo("MessageValue"));

            // 2メッセージ目検証
            List resultTuple2 = (List) resultList.get(1);

            assertThat(resultTuple2.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple2.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey2]"));
            assertThat(resultTuple2.get(1).toString(), equalTo("MessageValue"));

            // 3メッセージ目検証
            List resultTuple3 = (List) resultList.get(2);

            assertThat(resultTuple3.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple3.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey3]"));
            assertThat(resultTuple3.get(1).toString(), equalTo("MessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ1個、キー指定、キー重複ありの状態で1メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー有_重複あり_1メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param1KeyHasDupKey1MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー有_重複あり_1メッセージメソッド用のTestJobクラス
     */
    private class Param1KeyHasDupKey1MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1KeyHasDupKey1MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message"), Arrays.asList("MessageKey")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ1個、キー指定、キー重複ありの状態で3メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー有_重複あり_3メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param1KeyHasDupKey3MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー有_重複あり_3メッセージメソッド用のTestJobクラス
     */
    private class Param1KeyHasDupKey3MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param1KeyHasDupKey3MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(
                    cluster,
                    createTopology(Arrays.asList("Message"),
                            Arrays.asList("ChildKey1", "MessageKey", "ChildKey3")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 3);

            // 1メッセージ目検証
            List resultTuple1 = (List) resultList.get(0);

            assertThat(resultTuple1.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple1.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey1]"));
            assertThat(resultTuple1.get(1).toString(), equalTo("MessageValue"));

            // 2メッセージ目検証
            List resultTuple2 = (List) resultList.get(1);

            assertThat(resultTuple2.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple2.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple2.get(1).toString(), equalTo("MessageValue"));

            // 3メッセージ目検証
            List resultTuple3 = (List) resultList.get(2);

            assertThat(resultTuple3.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple3.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey3]"));
            assertThat(resultTuple3.get(1).toString(), equalTo("MessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ1個、キーなしの状態でメッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ1個_キー無し()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
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

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message"), null),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ3個、キー指定、キー重複なしの状態で1メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー有_重複なし_1メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3KeyHasUnique1MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ3個_キー有_重複なし_1メッセージメソッド用のTestJobクラス
     */
    private class Param3KeyHasUnique1MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3KeyHasUnique1MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(
                    cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3"),
                            Arrays.asList("ChildKey")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey, ChildKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple.get(3).toString(), equalTo("MessageValue3"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ3個、キー指定、キー重複なしの状態で3メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー有_重複なし_3メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3KeyHasUnique3MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ3個_キー有_重複なし_3メッセージメソッド用のTestJobクラス
     */
    private class Param3KeyHasUnique3MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3KeyHasUnique3MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(
                    cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3"),
                            Arrays.asList("ChildKey1", "ChildKey2", "ChildKey3")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(3, resultList.size());

            // 1メッセージ目検証
            List resultTuple1 = (List) resultList.get(0);

            assertThat(resultTuple1.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple1.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey1]"));
            assertThat(resultTuple1.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple1.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple1.get(3).toString(), equalTo("MessageValue3"));

            // 2メッセージ目検証
            List resultTuple2 = (List) resultList.get(1);

            assertThat(resultTuple2.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple2.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey2]"));
            assertThat(resultTuple2.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple2.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple2.get(3).toString(), equalTo("MessageValue3"));

            // 3メッセージ目検証
            List resultTuple3 = (List) resultList.get(2);

            assertThat(resultTuple3.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple3.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey3]"));
            assertThat(resultTuple3.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple3.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple3.get(3).toString(), equalTo("MessageValue3"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ3個、キー指定、キー重複ありの状態で1メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー有_重複あり_1メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3KeyHasDupKey1MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ3個_キー有_重複あり_1メッセージメソッド用のTestJobクラス
     */
    private class Param3KeyHasDupKey1MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3KeyHasDupKey1MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(
                    cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3"),
                            Arrays.asList("MessageKey")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple.get(3).toString(), equalTo("MessageValue3"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ3個、キー指定、キー重複ありの状態で3メッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー有_重複あり_3メッセージ()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3KeyHasDupKey3MsgTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ3個_キー有_重複あり_3メッセージメソッド用のTestJobクラス
     */
    private class Param3KeyHasDupKey3MsgTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3KeyHasDupKey3MsgTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(
                    cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3"),
                            Arrays.asList("ChildKey1", "MessageKey", "ChildKey3")),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 3);

            // 1メッセージ目検証
            List resultTuple1 = (List) resultList.get(0);

            assertThat(resultTuple1.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple1.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey1]"));
            assertThat(resultTuple1.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple1.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple1.get(3).toString(), equalTo("MessageValue3"));

            // 2メッセージ目検証
            List resultTuple2 = (List) resultList.get(1);

            assertThat(resultTuple2.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple2.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple2.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple2.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple2.get(3).toString(), equalTo("MessageValue3"));

            // 3メッセージ目検証
            List resultTuple3 = (List) resultList.get(2);

            assertThat(resultTuple3.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple3.get(0).toString(),
                    equalTo("KeyHistory=[MessageKey, ChildKey3]"));
            assertThat(resultTuple3.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple3.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple3.get(3).toString(), equalTo("MessageValue3"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * Bolt>Boltのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(List)}
     * @test キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     *    condition:: パラメータ3個、キーなしの状態でメッセージ送信する
     *    result:: キー情報履歴、メッセージが次のBoltに指定したキーで配信されること
     */
    @Test
    public void testEmit_パラメータ3個_キー無し()
    {
        // 準備
        KeyHistory keyInfo = new KeyHistory();
        keyInfo.addKey("MessageKey");
        this.mockedSources.addMockData("KeyTraceThroughSpout", new Values(keyInfo, "MessageValue1",
                "MessageValue2", "MessageValue3"));

        // 実施
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new Param3NoKeyTestJob());

        // 検証
        assertTrue(this.isAssert);
    }

    /**
     * testEmit_パラメータ1個_キー無しメソッド用のTestJobクラス
     */
    private class Param3NoKeyTestJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        Param3NoKeyTestJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 実施
            Map result = Testing.completeTopology(cluster,
                    createTopology(Arrays.asList("Message1", "Message2", "Message3"), null),
                    AmBaseBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "KeyTraceThroughBolt");

            // KeyTraceThroughBoltでは「Message1」「Message2」「Message3」というフィールドを下流に送付しているため、
            // KeyTraceThroughBoltが送付するフィールドに値が入っていればKeyTraceThroughBoltWithKey>KeyTraceThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.get(0), instanceOf(KeyHistory.class));
            assertThat(resultTuple.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
            assertThat(resultTuple.get(1).toString(), equalTo("MessageValue1"));
            assertThat(resultTuple.get(2).toString(), equalTo("MessageValue2"));
            assertThat(resultTuple.get(3).toString(), equalTo("MessageValue3"));

            AmBaseBoltUnitTest.this.isAssert = true;
        }
    }

    /**
     * 下記の構成を保持する検証用Topology定義を作成する。<br>
     * <ol>
     * <li>KeyTraceThroughSpout : 指定したフィールドを指定してグルーピングを定義するSpout</li>
     * <li>KeyTraceThroughBoltWithKey : 指定したフィールドを指定し、キーを追加してメッセージを送信するBolt</li>
     * <li>KeyTraceThroughBolt : 指定したフィールドを指定してメッセージを送信するBolt</li>
     * </ol>
     *
     * @param fields 指定フィールド
     * @param keys 指定キー
     * @return 該当のTopology定義
     */
    private StormTopology createTopology(List<String> fields, List<String> keys)
    {
        TopologyBuilder builder = new TopologyBuilder();

        // Add Spout(BlankAmBaseSpout)
        BlankAmBaseSpout blankAmBaseSpout = new BlankAmBaseSpout();
        builder.setSpout("BlankAmBaseSpout", blankAmBaseSpout);

        // Add Bolt(KeyTraceThroughSpout -> KeyTraceThroughBoltWithKey)
        AmBaseThroughBolt throughBoltWithKey = new AmBaseThroughBolt();
        throughBoltWithKey.setFields(fields);
        throughBoltWithKey.setKeys(keys);
        builder.setBolt("KeyTraceThroughBoltWithKey", throughBoltWithKey).shuffleGrouping(
                "KeyTraceThroughSpout");

        // Add Bolt(KeyTraceThroughBoltWithKey -> KeyTraceThroughBolt)
        AmBaseThroughBolt throughBolt = new AmBaseThroughBolt();
        throughBolt.setFields(fields);
        builder.setBolt("KeyTraceThroughBolt", throughBolt).shuffleGrouping(
                "KeyTraceThroughBoltWithKey");

        StormTopology topology = builder.createTopology();
        return topology;
    }

    /**
     * 下記の構成を保持する検証用Topology定義を作成する。<br>
     * <ol>
     * <li>KeyTraceThroughSpout : 指定したフィールドを指定してグルーピングを定義するSpout</li>
     * <li>KeyTraceThroughBoltWithKey : 指定したフィールドを指定し、キーを追加してメッセージを送信するBolt</li>
     * <li>KeyTraceThroughBolt : 指定したフィールドを指定してメッセージを送信するBolt</li>
     * </ol>
     *
     * @param fields 指定フィールド
     * @param keys 指定キー
     * @return 該当のTopology定義
     */
    private StormTopology createAckTopology(List<String> fields, List<String> keys)
    {
        TopologyBuilder builder = new TopologyBuilder();

        // Build Topology
        // Add Spout(BlankAmBaseSpout)
        BlankAmBaseSpout blankAmBaseSpout = new BlankAmBaseSpout();
        builder.setSpout("BlankAmBaseSpout", blankAmBaseSpout);

        // Add Bolt(BlankAmBaseSpout -> KeyTraceThroughBoltWithKey)
        AmBaseThroughBolt throughBoltWithKey = new AmBaseThroughBolt();
        throughBoltWithKey.setFields(fields);
        throughBoltWithKey.setKeys(keys);
        throughBoltWithKey.setManualAck(true);
        builder.setBolt("KeyTraceThroughBoltWithKey", throughBoltWithKey).shuffleGrouping(
                "KeyTraceThroughSpout");

        // Add Bolt(KeyTraceThroughBoltWithKey -> KeyTraceThroughBolt)
        AmBaseThroughBolt throughBolt = new AmBaseThroughBolt();
        throughBolt.setFields(fields);
        builder.setBolt("KeyTraceThroughBolt", throughBolt).shuffleGrouping(
                "KeyTraceThroughBoltWithKey");

        StormTopology topology = builder.createTopology();
        return topology;
    }
}
