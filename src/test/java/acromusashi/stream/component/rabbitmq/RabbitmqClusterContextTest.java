package acromusashi.stream.component.rabbitmq;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class RabbitmqClusterContextTest
{
    /** 例外出力確認用Ruleインスタンス */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * 情報の保持
     * 
     * @target {@link RabbitmqClusterContext#setMqProcessList(List)}
     * @test 情報の保持を確認する。(RabbitMQプロセス一覧null:例外出力)
     *    condition:: RabbitMQプロセス一覧：null
     *    result:: 例外が出力されること
     * 
     * @test 情報の保持を確認する。(RabbitMQプロセス一覧null:例外メッセージ)
     *    condition:: RabbitMQプロセス一覧：null
     *    result:: 例外メッセージが適切であること
     * 
     * @test 情報の保持を確認する。(RabbitMQプロセス一覧空:例外出力)
     *    condition:: RabbitMQプロセス一覧：空
     *    result:: 例外が出力されること
     * 
     * @test 情報の保持を確認する。(RabbitMQプロセス一覧空:例外メッセージ)
     *    condition:: RabbitMQプロセス一覧：空
     *    result:: 例外メッセージが適切であること
     * 
     * @test 情報の保持を確認する。(RabbitMQプロセス一覧要素1個:インスタンス設定)
     *    condition:: RabbitMQプロセス一覧：要素1個
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(RabbitMQプロセス一覧要素2個:インスタンス設定)
     *    condition:: RabbitMQプロセス一覧：要素2個
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(呼出元別、接続先RabbitMQプロセスの定義マップに定義されているプロセスを定義していないRabbitMQプロセス一覧:例外出力)
     *    condition:: RabbitMQプロセス一覧：呼出元別、接続先RabbitMQプロセスの定義マップに定義されているプロセスを定義していない
     *    result:: 例外が出力されること
     * 
     * @test 情報の保持を確認する。(呼出元別、接続先RabbitMQプロセスの定義マップに定義されているプロセスを定義していないRabbitMQプロセス一覧:例外メッセージ)
     *    condition:: RabbitMQプロセス一覧：呼出元別、接続先RabbitMQプロセスの定義マップに定義されているプロセスを定義していない
     *    result:: 例外メッセージが適切であること
     */
    @DataPoints
    public static Fixture[] createFixture_MqProcessList()
    {
        List<Fixture> patterns = new ArrayList<Fixture>();

        // null
        patterns.add(new Fixture((LinkedList<String>) null, new RabbitmqCommunicateException(
                "ProcessList is not defined.")));

        // 空リスト
        patterns.add(new Fixture(new LinkedList<String>(), new RabbitmqCommunicateException(
                "ProcessList is not defined.")));

        // 要素1個
        patterns.add(new Fixture(new LinkedList<String>(Arrays.asList("localhost:5672"))));

        // 要素2個
        patterns.add(new Fixture(new LinkedList<String>(Arrays.asList("localhost:5672",
                "127.0.0.1:5673"))));

        // 呼出元別、接続先RabbitMQプロセスの定義マップに定義されているプロセスを定義していない
        Map<String, String> presetConnectionProcessMap = new HashMap<String, String>();
        presetConnectionProcessMap.put("localhost_normal_0", Fixture.NORMAL_MQ_PROCESS_LIST.get(0));

        patterns.add(new Fixture(new LinkedList<String>(Arrays.asList("127.0.0.1:5673")),
                presetConnectionProcessMap, new RabbitmqCommunicateException(
                        "Connection process is illegal. NonContainedProcessSet=["
                                + Fixture.NORMAL_MQ_PROCESS_LIST.get(0) + "]")));

        return patterns.toArray(new Fixture[patterns.size()]);
    }

    /**
     * 情報の保持
     * 
     * @target {@link RabbitmqClusterContext#setQueueList(List)}
     * @test 情報の保持を確認する。(キュー一覧null:例外出力)
     *    condition:: キュー一覧：null
     *    result:: 例外が出力されること
     * 
     * @test 情報の保持を確認する。(キュー一覧null:例外メッセージ)
     *    condition:: キュー一覧：null
     *    result:: 例外メッセージが適切であること
     * 
     * @test 情報の保持を確認する。(キュー一覧空:例外出力)
     *    condition:: キュー一覧：空
     *    result:: 例外が出力されること
     * 
     * @test 情報の保持を確認する。(キュー一覧空:例外メッセージ)
     *    condition:: キュー一覧：空
     *    result:: 例外メッセージが適切であること
     * 
     * @test 情報の保持を確認する。(キュー一覧要素1個:インスタンス設定)
     *    condition:: キュー一覧：要素1個
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(キュー一覧要素2個:インスタンス設定)
     *    condition:: キュー一覧：要素2個
     *    result:: インスタンスに設定されること。
     */
    @DataPoints
    public static Fixture[] createFixture_QueueList()
    {
        List<Fixture> patterns = new ArrayList<Fixture>();

        // null 
        patterns.add(new Fixture((ArrayList<String>) null, new RabbitmqCommunicateException(
                "QueueList is not defined.")));

        // 空
        patterns.add(new Fixture(new ArrayList<String>(), new RabbitmqCommunicateException(
                "QueueList is not defined.")));

        // 要素1個
        patterns.add(new Fixture(new ArrayList<String>(Arrays.asList("normal_0"))));

        // 要素2個
        patterns.add(new Fixture(new ArrayList<String>(Arrays.asList("normal_0", "normal_1"))));

        return patterns.toArray(new Fixture[patterns.size()]);
    }

    /**
     * 情報の保持
     * 
     * @target {@link RabbitmqClusterContext#setConnectionProcessMap(Map)}
     * @test 情報の保持を確認する。(呼出元別、接続先RabbitMQプロセスの定義マップnull:インスタンス設定)
     *    condition:: 呼出元別、接続先RabbitMQプロセスの定義マップ：null
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(呼出元別、接続先RabbitMQプロセスの定義マップ空:インスタンス設定)
     *    condition:: 呼出元別、接続先RabbitMQプロセスの定義マップ：空
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(呼出元別、接続先RabbitMQプロセスの定義マップ要素1個:インスタンス設定)
     *    condition:: 呼出元別、接続先RabbitMQプロセスの定義マップ：要素1個
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(呼出元別、接続先RabbitMQプロセスの定義マップ要素2個:インスタンス設定)
     *    condition:: 呼出元別、接続先RabbitMQプロセスの定義マップ：要素2個
     *    result:: インスタンスに設定されること。
     * 
     * @test 情報の保持を確認する。(プロセス一覧に定義されているプロセスを定義していない、呼出元別、接続先RabbitMQプロセスの定義マップnull:例外出力)
     *    condition:: 呼出元別、接続先RabbitMQプロセスの定義マップ：プロセス一覧に定義されているプロセスを定義していない
     *    result:: 例外が出力されること
     * 
     * @test 情報の保持を確認する。(プロセス一覧に定義されているプロセスを定義していない、呼出元別、接続先RabbitMQプロセスの定義マップnull:例外メッセージ)
     *    condition:: 呼出元別、接続先RabbitMQプロセスの定義マップ：プロセス一覧に定義されているプロセスを定義していない
     *    result:: 例外メッセージが適切であること
     */
    @DataPoints
    public static Fixture[] createFixture_ConnectionProcessMap()
    {
        List<Fixture> patterns = new ArrayList<Fixture>();

        // null
        patterns.add(new Fixture((HashMap<String, String>) null));

        // 空
        patterns.add(new Fixture(new HashMap<String, String>()));

        // 要素1個
        HashMap<String, String> connectionProcessMap_value1 = new HashMap<String, String>();
        connectionProcessMap_value1.put("localhost_normal_0", Fixture.NORMAL_MQ_PROCESS_LIST.get(0));
        patterns.add(new Fixture(connectionProcessMap_value1));

        // 要素2個
        HashMap<String, String> connectionProcessMap_value2 = new HashMap<String, String>();
        connectionProcessMap_value2.put("localhost_normal_0", Fixture.NORMAL_MQ_PROCESS_LIST.get(0));
        connectionProcessMap_value2.put("localhost_normal_1", Fixture.NORMAL_MQ_PROCESS_LIST.get(1));
        patterns.add(new Fixture(connectionProcessMap_value2));

        // プロセス一覧に定義されているプロセスを定義していない
        LinkedList<String> mqProcessList = new LinkedList<String>(Arrays.asList("127.0.0.1:5673"));
        Map<String, String> presetConnectionProcessMap = new HashMap<String, String>();
        presetConnectionProcessMap.put("localhost_normal_0", Fixture.NORMAL_MQ_PROCESS_LIST.get(0));
        patterns.add(new Fixture(mqProcessList, presetConnectionProcessMap,
                new RabbitmqCommunicateException(
                        "Connection process is illegal. NonContainedProcessSet=["
                                + Fixture.NORMAL_MQ_PROCESS_LIST.get(0) + "]")));

        return patterns.toArray(new Fixture[patterns.size()]);
    }

    @Theory
    public void testSetMqProcessList_NormalCase(Fixture fixture)
            throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.context.getMqProcessList() != fixture.mqProcessList);
        Assume.assumeTrue(fixture.expectedException == null);

        // 準備
        RabbitmqClusterContext context = fixture.context;
        List<String> mqProcessList = fixture.mqProcessList;
        Map<String, String> connectionProcessMap = fixture.connectionProcessMap;

        // 別の呼出元別、接続先RabbitMQプロセスのマップが存在するならば、それが設定されているようにする。
        if (context.getConnectionProcessMap() != connectionProcessMap)
        {
            context.setConnectionProcessMap(connectionProcessMap);
        }

        List<String> expected = new ArrayList<String>(mqProcessList);

        // 実施
        context.setMqProcessList(mqProcessList);

        // 検証
        assertThat(context.getMqProcessList().toString(), is(expected.toString()));
    }

    @Theory
    public void testSetMqProcessList_ExceptionCase(Fixture fixture)
            throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.context.getMqProcessList() != fixture.mqProcessList);
        Assume.assumeTrue(fixture.expectedException != null);

        // 準備
        RabbitmqClusterContext context = fixture.context;
        List<String> mqProcessList = fixture.mqProcessList;
        Map<String, String> connectionProcessMap = fixture.connectionProcessMap;

        // 別の呼出元別、接続先RabbitMQプロセスのマップが存在するならば、それが設定されているようにする。
        if (context.getConnectionProcessMap() != connectionProcessMap)
        {
            context.setConnectionProcessMap(connectionProcessMap);
        }

        expectedException.expect(fixture.expectedException.getClass());
        expectedException.expectMessage(fixture.expectedException.getMessage());

        // 実施
        context.setMqProcessList(mqProcessList);
    }

    @Theory
    public void testSetQueueList_NormalCase(Fixture fixture) throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.context.getQueueList() != fixture.queueList);
        Assume.assumeTrue(fixture.expectedException == null);

        // 準備
        RabbitmqClusterContext context = fixture.context;
        List<String> queueList = fixture.queueList;
        List<String> expected = new ArrayList<String>(queueList);

        // 実施
        context.setQueueList(queueList);

        // 検証
        assertThat(context.getQueueList().toString(), is(expected.toString()));
    }

    @Theory
    public void testSetQueueList_ExceptionCase(Fixture fixture) throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.context.getQueueList() != fixture.queueList);
        Assume.assumeTrue(fixture.expectedException != null);

        // 準備
        RabbitmqClusterContext context = fixture.context;
        List<String> queueList = fixture.queueList;
        expectedException.expect(fixture.expectedException.getClass());
        expectedException.expectMessage(fixture.expectedException.getMessage());

        // 実施
        context.setQueueList(queueList);
    }

    @Theory
    public void testSetConnectionProcessMap_NormalCase(Fixture fixture)
            throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.context.getConnectionProcessMap() != fixture.connectionProcessMap);
        Assume.assumeTrue(fixture.expectedException == null);

        // 準備
        RabbitmqClusterContext context = fixture.context;
        List<String> mqProcessList = fixture.mqProcessList;
        Map<String, String> connectionProcessMap = fixture.connectionProcessMap;

        // 別の呼出元別、接続先RabbitMQプロセスのマップが存在するならば、それが設定されているようにする。
        if (context.getMqProcessList() != mqProcessList)
        {
            context.setMqProcessList(mqProcessList);
        }

        Map<String, String> expected = context.getConnectionProcessMap();
        if (connectionProcessMap != null)
        {
            expected = new HashMap<String, String>(connectionProcessMap);
        }

        // 実施
        context.setConnectionProcessMap(connectionProcessMap);

        // 検証
        assertThat(context.getConnectionProcessMap().toString(), is(expected.toString()));
    }

    @Theory
    public void testSetConnectionProcessMap_ExceptionCase(Fixture fixture)
            throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.context.getConnectionProcessMap() != fixture.connectionProcessMap);
        Assume.assumeTrue(fixture.expectedException != null);

        // 準備
        RabbitmqClusterContext context = fixture.context;
        List<String> mqProcessList = fixture.mqProcessList;
        Map<String, String> connectionProcessMap = fixture.connectionProcessMap;

        // 別の呼出元別、接続先RabbitMQプロセスのマップが存在するならば、それが設定されているようにする。
        if (context.getMqProcessList() != mqProcessList)
        {
            context.setMqProcessList(mqProcessList);
        }

        expectedException.expect(fixture.expectedException.getClass());
        expectedException.expectMessage(fixture.expectedException.getMessage());

        // 実施
        context.setConnectionProcessMap(connectionProcessMap);
    }

    private static class Fixture
    {

        /** 登録可能なRabbitMQプロセス一覧 */
        private static LinkedList<String>  NORMAL_MQ_PROCESS_LIST        = new LinkedList<String>(
                                                                                 Arrays.asList("localhost:5672"));

        /** 登録可能なキュー一覧 */
        private static List<String>        NORMAL_QUEUE_LIST             = Arrays.asList("normal_0");

        /** 登録可能な呼出元別、接続先RabbitMQプロセスの定義マップ */
        private static Map<String, String> NORMAL_CONNECTION_PROCESS_MAP = new HashMap<String, String>();

        /** 定義ファイルから読み込んだ情報 */
        RabbitmqClusterContext             context                       = createNormalContext();

        /** RabbitMQプロセス一覧 */
        List<String>                       mqProcessList                 = NORMAL_MQ_PROCESS_LIST;

        /** キュー一覧 */
        List<String>                       queueList                     = NORMAL_QUEUE_LIST;

        /** 呼出元別、接続先RabbitMQプロセスの定義マップ */
        Map<String, String>                connectionProcessMap          = NORMAL_CONNECTION_PROCESS_MAP;

        /** 期待値(例外) */
        final Exception                    expectedException;

        /**
         * @param mqProcessList
         */
        public Fixture(LinkedList<String> mqProcessList)
        {
            this.mqProcessList = mqProcessList;
            this.expectedException = null;
        }

        /**
         * @param mqProcessList
         * @param expectedException
         */
        public Fixture(LinkedList<String> mqProcessList, Exception expectedException)
        {
            this.mqProcessList = mqProcessList;
            this.expectedException = expectedException;
        }

        public Fixture(LinkedList<String> mqProcessList, Map<String, String> connectionProcessMap,
                Exception expectedException)
        {
            this.mqProcessList = mqProcessList;
            this.connectionProcessMap = connectionProcessMap;
            this.expectedException = expectedException;
        }

        public Fixture(ArrayList<String> queueList)
        {
            this.queueList = queueList;
            this.expectedException = null;
        }

        public Fixture(ArrayList<String> queueList, Exception expectedException)
        {
            this.queueList = queueList;
            this.expectedException = expectedException;
        }

        public Fixture(Map<String, String> connectionProcessMap)
        {
            this.connectionProcessMap = connectionProcessMap;
            this.expectedException = null;
        }

        private RabbitmqClusterContext createNormalContext()
        {
            RabbitmqClusterContext context = new RabbitmqClusterContext();
            NORMAL_MQ_PROCESS_LIST = new LinkedList<String>(Arrays.asList("localhost:5672",
                    "127.0.0.1:5673"));
            NORMAL_QUEUE_LIST = Arrays.asList("normal_0");
            NORMAL_CONNECTION_PROCESS_MAP = new HashMap<String, String>();

            try
            {
                context.setMqProcessList(NORMAL_MQ_PROCESS_LIST);
                context.setQueueList(NORMAL_QUEUE_LIST);
                context.setConnectionProcessMap(NORMAL_CONNECTION_PROCESS_MAP);
            }
            catch (RabbitmqCommunicateException ex)
            {
                ex.printStackTrace();
                return null;
            }

            return context;
        }
    }

}
