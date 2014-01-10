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
package acromusashi.stream.component.rabbitmq;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
public class AbstractContextBuilderTest
{

    /** 例外出力確認用Ruleインスタンス */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * 設定の解析
     * 
     * @target {@link AbstractContextBuilder#AbstractContextBuilder(List)}
     * @test 設定の解析を確認する。(null)
     *    condition:: RabbitMQクラスタコンテキストのリスト:null
     *    result:: 例外が出力されること
     * 
     * @test 設定の解析を確認する。(空)
     *    condition:: RabbitMQクラスタコンテキストのリスト:空
     *    result:: 例外が出力されること
     * 
     * @test 設定の解析を確認する。(クラスタ構成1個：クラスタ構成コンテキストMap)
     *    condition:: RabbitMQクラスタコンテキストのリスト:要素1個
     *    result:: キューを保持するクラスタの構成定義が初期化されること
     * 
     * @test 設定の解析を確認する。(クラスタ構成1個：RabbitMQプロセス一覧)
     *    condition:: RabbitMQクラスタコンテキストのリスト:要素1個
     *    result:: キューを保持するクラスタのRabbitMQプロセス一覧が初期化されること
     * 
      * @test 設定の解析を確認する。(クラスタ構成2個：クラスタ構成コンテキストMap)
     *    condition:: RabbitMQクラスタコンテキストのリスト:要素2個
     *    result:: キューを保持するクラスタの構成定義が初期化されること
     * 
     * @test 設定の解析を確認する。(クラスタ構成2個：RabbitMQプロセス一覧)
     *    condition:: RabbitMQクラスタコンテキストのリスト:要素2個
     *    result:: キューを保持するクラスタのRabbitMQプロセス一覧が初期化されること
     * 
     * @test 設定の解析を確認する。(クラスタ構成2個(キュー名に重複あり)：例外出力)
     *    condition:: RabbitMQクラスタコンテキストのリスト:要素2個(キュー名に重複あり)
     *    result:: 例外が出力されること
     * 
     * @test 設定の解析を確認する。(クラスタ構成2個(キュー名に重複あり)：例外メッセージ)
     *    condition:: RabbitMQクラスタコンテキストのリスト:要素2個(キュー名に重複あり)
     *    result:: 例外メッセージが適切であること
     * 
     */
    @DataPoints
    public static Fixture[] createFixture_ContextList()
    {
        // 呼出元名は固定（並び替えを行わない）
        String clientId = null;

        List<Fixture> patterns = new ArrayList<Fixture>();

        // null
        patterns.add(new Fixture((List<RabbitmqClusterContext>) null, clientId,
                new NullPointerException()));

        // 空
        patterns.add(new Fixture(new ArrayList<RabbitmqClusterContext>(), clientId,
                new RabbitmqCommunicateException("RabbitMqClusterContext is empty.")));

        // 要素１個
        RabbitmqClusterContext context_value1 = new RabbitmqClusterContext();
        try
        {
            context_value1.setMqProcessList(Arrays.asList("localhost:5672"));
            context_value1.setQueueList(Arrays.asList("normal_0"));
        }
        catch (RabbitmqCommunicateException ex)
        {
            ex.printStackTrace();
        }

        HashMap<String, RabbitmqClusterContext> expectedContextMap_value1 = new HashMap<String, RabbitmqClusterContext>();
        expectedContextMap_value1.put("normal_0", context_value1);
        HashMap<String, List<String>> expectedProcessLists_value1 = new HashMap<String, List<String>>();
        expectedProcessLists_value1.put("normal_0", Arrays.asList("localhost:5672"));
        patterns.add(new Fixture(Arrays.asList(context_value1), clientId,
                expectedContextMap_value1, expectedProcessLists_value1));

        // 要素2個
        RabbitmqClusterContext context_value2_1 = new RabbitmqClusterContext();
        RabbitmqClusterContext context_value2_2 = new RabbitmqClusterContext();
        try
        {
            context_value2_1.setMqProcessList(Arrays.asList("localhost:5672"));
            context_value2_1.setQueueList(Arrays.asList("normal_0"));
            context_value2_2.setMqProcessList(Arrays.asList("127.0.0.1:5673"));
            context_value2_2.setQueueList(Arrays.asList("normal_1"));
        }
        catch (RabbitmqCommunicateException ex)
        {
            ex.printStackTrace();
        }

        HashMap<String, RabbitmqClusterContext> expectedContextMap_value2 = new HashMap<String, RabbitmqClusterContext>();
        expectedContextMap_value2.put("normal_0", context_value2_1);
        expectedContextMap_value2.put("normal_1", context_value2_2);
        HashMap<String, List<String>> expectedProcessLists_value2 = new HashMap<String, List<String>>();
        expectedProcessLists_value2.put("normal_0", Arrays.asList("localhost:5672"));
        expectedProcessLists_value2.put("normal_1", Arrays.asList("127.0.0.1:5673"));
        patterns.add(new Fixture(Arrays.asList(context_value2_1, context_value2_2), clientId,
                expectedContextMap_value2, expectedProcessLists_value2));

        // 要素2個（キュー名に重複あり）
        RabbitmqClusterContext context_value2_duplicateQueue_1 = new RabbitmqClusterContext();
        RabbitmqClusterContext context_value2_duplicateQueue_2 = new RabbitmqClusterContext();
        try
        {
            context_value2_duplicateQueue_1.setMqProcessList(Arrays.asList("localhost:5672"));
            context_value2_duplicateQueue_1.setQueueList(Arrays.asList("normal_0"));
            context_value2_duplicateQueue_2.setMqProcessList(Arrays.asList("127.0.0.1:5673"));
            context_value2_duplicateQueue_2.setQueueList(Arrays.asList("normal_0"));
        }
        catch (RabbitmqCommunicateException ex)
        {
            ex.printStackTrace();
        }

        patterns.add(new Fixture(Arrays.asList(context_value2_duplicateQueue_1,
                context_value2_duplicateQueue_2), clientId, new RabbitmqCommunicateException(
                "QueueName is not unique. QueueName=normal_0")));

        return patterns.toArray(new Fixture[patterns.size()]);
    }

    /**
     * 設定の解析
     * 
     * @target {@link AbstractContextBuilder#AbstractContextBuilder(List)}
     * @test 設定の解析を確認する。(クライアントID null：クラスタ構成コンテキストMap)
     *    condition:: 呼出元名:null
     *    result:: キューを保持するクラスタの構成定義が初期化されること
     * 
     * @test 設定の解析を確認する。(クライアントID null：RabbitMQプロセス一覧)
     *    condition:: 呼出元名:null
     *    result:: キューを保持するクラスタのRabbitMQプロセス一覧が初期化されること
     * 
      * @test 設定の解析を確認する。(空文字：クラスタ構成コンテキストMap)
     *    condition:: 呼出元名:空文字
     *    result:: キューを保持するクラスタの構成定義が初期化されること
     * 
     * @test 設定の解析を確認する。(空文字：RabbitMQプロセス一覧)
     *    condition:: 呼出元名:空文字
     *    result:: キューを保持するクラスタのRabbitMQプロセス一覧が初期化されること
     * 
     * @test 設定の解析を確認する。(呼出元別、接続先RabbitMQプロセスの定義Mapに存在する、呼出元名：クラスタ構成コンテキストMap)
     *    condition:: 呼出元名:呼出元別、接続先RabbitMQプロセスの定義Mapに存在する、呼出元名
     *    result:: キューを保持するクラスタの構成定義が初期化されること
     * 
     * @test 設定の解析を確認する。(呼出元別、接続先RabbitMQプロセスの定義Mapに存在する、呼出元名：RabbitMQプロセス一覧)
     *    condition:: 呼出元名:呼出元別、接続先RabbitMQプロセスの定義Mapに存在する、呼出元名
     *    result:: キューを保持するクラスタのRabbitMQプロセス一覧が初期化されること
     * 
     * @test 設定の解析を確認する。(呼出元別、接続先RabbitMQプロセスの定義Mapに存在しない、呼出元名：クラスタ構成コンテキストMap)
     *    condition:: 呼出元名:呼出元別、接続先RabbitMQプロセスの定義Mapに存在しない、呼出元名
     *    result:: キューを保持するクラスタの構成定義が初期化されること
     * 
     * @test 設定の解析を確認する。(呼出元別、接続先RabbitMQプロセスの定義Mapに存在しない、呼出元名：RabbitMQプロセス一覧)
     *    condition:: 呼出元名:呼出元別、接続先RabbitMQプロセスの定義Mapに存在しない、呼出元名
     *    result:: キューを保持するクラスタのRabbitMQプロセス一覧が初期化されること
     * 
     * @test 設定の解析を確認する。(例外発生)
     *    condition:: 呼出元名取得時に例外発生
     *    result:: 例外が出力されること
     */
    @DataPoints
    public static Fixture[] createFixture_ClientId()
    {
        // クラスタコンテキストの一覧は固定
        RabbitmqClusterContext context = new RabbitmqClusterContext();
        String orderdQueueName = "normal_0";
        String noOrderedQueueName = "normal_1";
        try
        {
            context.setMqProcessList(Arrays.asList("localhost:5672", "127.0.0.1:5672"));
            context.setQueueList(Arrays.asList(orderdQueueName, noOrderedQueueName));
            HashMap<String, String> connectionProcessMap = new HashMap<String, String>();
            connectionProcessMap.put("existClientId_" + orderdQueueName, "127.0.0.1:5672");
            context.setConnectionProcessMap(connectionProcessMap);
        }
        catch (RabbitmqCommunicateException ex)
        {
            ex.printStackTrace();
        }
        List<RabbitmqClusterContext> contextList_value1 = Arrays.asList(context);

        // クラスタコンテキストのマップは呼出元名により、変化しない。
        HashMap<String, RabbitmqClusterContext> expectedContextMap = new HashMap<String, RabbitmqClusterContext>();
        expectedContextMap.put(orderdQueueName, context);
        expectedContextMap.put(noOrderedQueueName, context);

        // 呼出元名によりRabbitMQプロセス一覧の並び替えが起きる場合
        HashMap<String, List<String>> expectedProcessLists_orderd = new HashMap<String, List<String>>();
        expectedProcessLists_orderd.put(orderdQueueName,
                Arrays.asList("127.0.0.1:5672", "localhost:5672"));
        expectedProcessLists_orderd.put(noOrderedQueueName,
                Arrays.asList("localhost:5672", "127.0.0.1:5672"));

        // 呼出元名によりRabbitMQプロセス一覧の並び替えが起きない場合
        HashMap<String, List<String>> expectedProcessLists_noOrderd = new HashMap<String, List<String>>();
        expectedProcessLists_noOrderd.put(orderdQueueName,
                Arrays.asList("localhost:5672", "127.0.0.1:5672"));
        expectedProcessLists_noOrderd.put(noOrderedQueueName,
                Arrays.asList("localhost:5672", "127.0.0.1:5672"));

        List<Fixture> patterns = new ArrayList<Fixture>();

        // null
        patterns.add(new Fixture(contextList_value1, "nonExistClientId", expectedContextMap,
                expectedProcessLists_noOrderd));

        // 空文字
        patterns.add(new Fixture(contextList_value1, "nonExistClientId", expectedContextMap,
                expectedProcessLists_noOrderd));

        // 呼出元別、接続先RabbitMQプロセスの定義Mapに存在する、呼出元名
        patterns.add(new Fixture(contextList_value1, "existClientId", expectedContextMap,
                expectedProcessLists_orderd));

        // 呼出元別、接続先RabbitMQプロセスの定義Mapに存在しない、呼出元名
        patterns.add(new Fixture(contextList_value1, "nonExistClientId", expectedContextMap,
                expectedProcessLists_noOrderd));

        // 例外発生
        patterns.add(new Fixture(contextList_value1, ContextBuilderImpl.EXCEPTION_OCCURED_HEADER,
                new RabbitmqCommunicateException()));

        return patterns.toArray(new Fixture[patterns.size()]);
    }

    @Theory
    public void testInitContextMap_NormalCase(Fixture fixture) throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.expectedException == null);

        // 準備
        List<RabbitmqClusterContext> contextList = fixture.contextList;
        ContextBuilderImpl.setClientIdHeader(fixture.clientIdHeader);
        Map<String, RabbitmqClusterContext> expectedContextMap = fixture.expectedContextMap;
        Map<String, List<String>> expectedProcessLists = fixture.expectedProcessLists;

        // 実施
        AbstractContextBuilder actualContextBuilder = new ContextBuilderImpl(contextList);

        // 検証
        assertThat(actualContextBuilder.getContextMap().toString(),
                is(expectedContextMap.toString()));
        assertThat(actualContextBuilder.getProcessLists().toString(),
                is(expectedProcessLists.toString()));
    }

    @Theory
    public void testInitContextMap_ExceptionCase(Fixture fixture)
            throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.expectedException != null);

        // 準備
        List<RabbitmqClusterContext> contextList = fixture.contextList;
        ContextBuilderImpl.setClientIdHeader(fixture.clientIdHeader);
        expectedException.expect(fixture.expectedException.getClass());
        if (fixture.expectedException.getMessage() != null)
        {
            expectedException.expectMessage(is(fixture.expectedException.getMessage()));
        }

        // 実施
        new ContextBuilderImpl(contextList);
    }

    private static class Fixture
    {

        /** クラスタ構成定義一覧 */
        List<RabbitmqClusterContext>        contextList;

        String                              clientIdHeader;

        /** キューを保持するクラスタの構成定義*/
        Map<String, RabbitmqClusterContext> expectedContextMap;

        /** キューを保持するクラスタのRabbitMQプロセス一覧 (フェイルオーバで接続する順番にした状態）*/
        Map<String, List<String>>           expectedProcessLists;

        /** 期待値(例外) */
        final Exception                     expectedException;

        /**
         * @param contextList
         * @param clientIdHeader 
         * @param expectedContextMap
         * @param expectedProcessLists
         */
        public Fixture(List<RabbitmqClusterContext> contextList, String clientIdHeader,
                Map<String, RabbitmqClusterContext> expectedContextMap,
                Map<String, List<String>> expectedProcessLists)
        {
            this.contextList = contextList;
            this.clientIdHeader = clientIdHeader;
            this.expectedContextMap = expectedContextMap;
            this.expectedProcessLists = expectedProcessLists;
            this.expectedException = null;
        }

        /**
         * @param contextList
         * @param clientIdHeader 
         * @param expectedException
         */
        public Fixture(List<RabbitmqClusterContext> contextList, String clientIdHeader,
                Exception expectedException)
        {
            this.contextList = contextList;
            this.clientIdHeader = clientIdHeader;
            this.expectedContextMap = null;
            this.expectedProcessLists = null;
            this.expectedException = expectedException;
        }
    }

    private static class ContextBuilderImpl extends AbstractContextBuilder
    {
        private static final String EXCEPTION_OCCURED_HEADER = "exceptionClientId";

        private static String       clientIdHeader;

        public ContextBuilderImpl(List<RabbitmqClusterContext> contextList)
                throws RabbitmqCommunicateException
        {
            super(contextList);
        }

        public static void setClientIdHeader(String setValue)
        {
            clientIdHeader = setValue;
        }

        @Override
        public String getClientId(String queueName) throws RabbitmqCommunicateException
        {
            if (EXCEPTION_OCCURED_HEADER.equals(clientIdHeader))
            {
                throw new RabbitmqCommunicateException();
            }
            return clientIdHeader + "_" + queueName;
        }
    }
}
