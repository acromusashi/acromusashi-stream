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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.Header;
import acromusashi.stream.entity.Message;
import acromusashi.stream.spout.ThroughSpout;
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
 * JacksonMessageConvertBoltのテストクラス<br>
 * 単一クラスではなく「Stormクラスタ」としての試験のため、JacksonMessageConvertBoltTestクラスとは別クラスとして作成する。
 */
public class JsonConvertBoltUnitTest
{
    /** 試験用ファイル配置ディレクトリ*/
    private static final String   DATA_DIR = "src/test/resources/"
                                                   + StringUtils.replaceChars(
                                                           JsonConvertBoltUnitTest.class.getPackage().getName(),
                                                           '.', '/') + '/';

    /** JUnit実行結果を保持する変数 */
    private boolean               isAssert_;

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
        this.isAssert_ = false;
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
     * 共通メッセージの変換に成功した場合のメッセージの送信内容を確認する
     * 
     * @target {@link JsonConvertBolt#execute(backtype.storm.tuple.Tuple)}
     * @test 変換Boltにて共通メッセージのエンティティが生成され、ストリームに送信されること
     *    condition:: 共通メッセージJSON形式のメッセージをSpoutにて取得
     *    result:: 変換Boltにて共通メッセージのエンティティが生成され、ストリームに送信されること
     */
    @Test
    public void testExecute_メッセージ変換成功() throws Exception
    {
        // 準備
        String messageKey = "192.168.100.31";
        String messageStr = FileUtils.readFileToString(new File(DATA_DIR
                + "JacksonMessageConvertBoltTest_Valid.txt"));
        this.mockedSources.addMockData("ThroughSpout", new Values(messageKey, messageStr));

        try
        {
            // 実施
            Testing.withSimulatedTimeLocalCluster(mkClusterParam, new SpreadAlarmConvertJob());
        }
        catch (Exception ex)
        {
            // Windows上でテストコードを実行するとLocalCluster終了時にZooKeeperの一時ファイル削除時に例外が発生するため、
            // 特定のメッセージを保持する例外の場合正常として扱う。
            if (ex.getMessage().startsWith("Unable to delete file") == false)
            {
                throw ex;
            }
        }

        // 検証結果確認
        assertTrue(this.isAssert_);
    }

    /**
     * testExecute_メッセージ変換成功メソッド用のTestJobクラス
     */
    private class SpreadAlarmConvertJob implements TestJob
    {
        /**
         * パラメータを指定せずにインスタンスを作成する。
         */
        SpreadAlarmConvertJob()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void run(ILocalCluster cluster) throws Exception
        {
            // 準備
            Message expected = baseExpectedMessage();

            // 実施
            Map result = Testing.completeTopology(cluster, createConvertTopology(),
                    JsonConvertBoltUnitTest.this.completeTopologyParam);

            // 検証
            List resultList = Testing.readTuples(result, "ThroughBolt");

            // ThroughBoltでは「Message」というフィールドを下流に送付しているため、
            // ThroughBoltが送付するフィールドに値が入っていればSpreadConvertBolt>ThroughBoltに送付されたことが確認できる。
            assertEquals(resultList.size(), 1);

            List resultTuple = (List) resultList.get(0);

            assertThat(resultTuple.size(), equalTo(2));
            assertThat(resultTuple.get(0).toString(), equalTo("192.168.100.31"));
            assertThat(resultTuple.get(1).toString(), equalTo(expected.toString()));

            JsonConvertBoltUnitTest.this.isAssert_ = true;
        }
    }

    /**
     * 下記の構成を保持する検証用Topology定義を作成する。<br>
     * <ol>
     * <li>ThroughSpout : 指定したフィールドを指定してメッセージを送信するSpout</li>
     * <li>JsonConvertBolt : 共通メッセージクラスにメッセージを変換するBolt</li>
     * <li>ThroughBolt : 指定したフィールドを指定してメッセージを送信するBolt</li>
     * </ol>
     * 
     * @return JacksonMessageConvertBolt検証用のTopology定義
     */
    private StormTopology createConvertTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();

        // Build Topology
        // Add Spout(ThroughSpout)
        ThroughSpout throughSpout = new ThroughSpout();
        throughSpout.setFields(Arrays.asList(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
        builder.setSpout("ThroughSpout", throughSpout);

        // Add Bolt(ThroughSpout -> JacksonMessageConvertBolt)
        JsonConvertBolt<TestUserEntity> convertBolt = new JsonConvertBolt<TestUserEntity>();
        convertBolt.setUserObjectClass(TestUserEntity.class);
        builder.setBolt("JsonConvertBolt", convertBolt).shuffleGrouping("ThroughSpout");

        // Add Bolt(JacksonMessageConvertBolt -> ThroughBolt)
        ThroughBolt throughBolt = new ThroughBolt();
        throughBolt.setFields(Arrays.asList(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
        builder.setBolt("ThroughBolt", throughBolt).shuffleGrouping("JsonConvertBolt");

        StormTopology topology = builder.createTopology();
        return topology;
    }

    /**
     * 検証用の共通メッセージエンティティを作成する
     * 
     * @return 共通メッセージエンティティ
     */
    private Message baseExpectedMessage()
    {
        Message message = new Message();
        Header header = new Header();
        header.setMessageId("192.168.100.31_20130419182101127_0019182101");
        header.setMessageKey("192.168.100.31");
        header.setSource("192.168.100.31");
        header.setType("snmp");
        header.setVersion("v2c");
        message.setHeader(header);
        message.setBody(createUserEntity());

        return message;
    }

    /**
     * 検証用のユーザエンティティを作成する
     * 
     * @return 検証用ユーザエンティティ
     */
    private TestUserEntity createUserEntity()
    {
        TestUserEntity entity = new TestUserEntity();

        entity.setMessageId("192.168.100.99_20130419182101127_0019182101");
        entity.setMessageKey("192.168.100.99");

        return entity;
    }
}
