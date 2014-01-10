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
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@RunWith(Enclosed.class)
public class DefaultRabbitmqClientTest
{
    private static final int RETRY_INTERVAL = 100;

    public static class DefaultRabbitmqClientTest_setInterval
    {

        /** 例外出力確認用Ruleインスタンス */
        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        /**
         * リトライ間隔の指定
         * 
         * @target {@link DefaultRabbitmqClient#setRetryInterval(int)}
         * @test リトライ間隔の指定を確認する。(リトライ間隔正の値)
         *    condition:: 正の値
         *    result:: リトライ間隔が設定されること
         */
        @Test
        public void testSetRetryInterval_正の値() throws RabbitmqCommunicateException
        {
            // 準備
            DefaultRabbitmqClient client = new DefaultRabbitmqClient();

            // 実施
            int expected = 1000;
            client.setRetryInterval(expected);

            // 検証
            int actual = client.getRetryInterval();
            assertEquals(expected, actual);
        }

        /**
         * リトライ間隔の指定
         * 
         * @target {@link DefaultRabbitmqClient#setRetryInterval(int)}
         * @test リトライ間隔の指定を確認する。(リトライ間隔境界値0：例外出力)
         *    condition:: 境界値0
         *    result:: 例外が出力されること
         * 
         * @test リトライ間隔の指定を確認する。(リトライ間隔境界値0：例外メッセージ)
         *    condition:: 境界値0
         *    result:: 例外メッセージが適切であること
         */
        @Test
        public void testSetRetryInterval_境界値() throws RabbitmqCommunicateException
        {
            // 準備
            int retryInterval = 0;
            DefaultRabbitmqClient client = new DefaultRabbitmqClient();
            Exception expected = new RabbitmqCommunicateException("RetryInterval is invalid. RetryInterval="
                    + retryInterval);
            expectedException.expect(expected.getClass());
            expectedException.expectMessage(is(expected.getMessage()));

            // 実施
            client.setRetryInterval(retryInterval);
        }

        /**
         * リトライ間隔の指定
         * 
         * @target {@link DefaultRabbitmqClient#setRetryInterval(int)}
         * @test リトライ間隔の指定を確認する。(リトライ間隔負の値：例外出力)
         *    condition:: 負の値
         *    result:: 例外が出力されること
         * 
         * @test リトライ間隔の指定を確認する。(リトライ間隔負の値：例外メッセージ)
         *    condition:: 負の値
         *    result:: 例外メッセージが適切であること
         */
        @Test
        public void testSetRetryInterval_負の値() throws RabbitmqCommunicateException
        {
            // 準備
            int retryInterval = -100;
            DefaultRabbitmqClient client = new DefaultRabbitmqClient();
            Exception expected = new RabbitmqCommunicateException(
                    "RetryInterval is invalid. RetryInterval=-100");
            expectedException.expect(expected.getClass());
            expectedException.expectMessage(is(expected.getMessage()));

            // 実施
            client.setRetryInterval(retryInterval);
        }
    }

    @RunWith(Theories.class)
    public static class DefaultRabbitmqClientTest_Send
    {

        /** 例外出力確認用Ruleインスタンス */
        @Rule
        public ExpectedException      expectedException           = ExpectedException.none();

        /** 定義されているキュー名 */
        public final static String    WELL_DEFINED_QUEUE_NAME     = "snmpTrap_0";

        /** 定義されていないキュー名 */
        private static final String   NOT_WELL_DEFINED_QUEUE_NAME = "dummyQueue";

        private static final Object   CONNECTION_FAIL_MESSAGE     = "connection fail";

        /** RabbitMQクライアント */
        private DefaultRabbitmqClient client;

        /**
         * メッセージの送信
         * 
         * @target {@link DefaultRabbitmqClient#send(String, Object)}
         * @test メッセージの送信を確認する。(定義されているキュー名:送信)
         *    condition:: 定義されているキュー名
         *    result:: 送信されること。
         * 
         * @test メッセージの送信を確認する。(定義されていないキュー名:例外出力)
         *    condition:: 定義されていないキュー名
         *    result:: 例外が出力されること
         * 
         * @test メッセージの送信を確認する。(定義されていないキュー名:例外メッセージ)
         *    condition:: 定義されていないキュー名
         *    result:: 例外メッセージが適切であること
         */
        @DataPoints
        public static Fixture[] createFixture_QueueName()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // 定義されているキュー名
            patterns.add(new Fixture(WELL_DEFINED_QUEUE_NAME, "message"));

            // 定義されていないキュー名
            patterns.add(new Fixture(NOT_WELL_DEFINED_QUEUE_NAME, "message",
                    new RabbitmqCommunicateException()));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        /**
         * メッセージの送信
         * 
         * @target {@link DefaultRabbitmqClient#send(String, Object)}
         * @test メッセージの送信を確認する。(メッセージnull:送信)
         *    condition:: メッセージがnull
         *    result:: 送信されること
         * 
         * @test メッセージの送信を確認する。(メッセージ空:送信)
         *    condition:: メッセージが空文字
         *    result:: 送信されること
         */
        @DataPoints
        public static Fixture[] createFixture_Message()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // null
            patterns.add(new Fixture(WELL_DEFINED_QUEUE_NAME, null));

            // 空文字
            patterns.add(new Fixture(WELL_DEFINED_QUEUE_NAME, ""));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        /**
         * メッセージの送信
         * 
         * @target {@link DefaultRabbitmqClient#send(String, Object)}
         * @test メッセージの送信を確認する。(接続エラー:例外出力)
         *    condition:: 接続エラーが発生
         *    result:: 例外が出力されること
         * 
         * @test メッセージの送信を確認する。(接続エラー:例外メッセージ)
         *    condition:: 接続エラーが発生
         *    result:: 例外メッセージが適切であること
         */
        @DataPoints
        public static Fixture[] createFixture_ConnectionError()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // 接続エラー
            patterns.add(new Fixture(WELL_DEFINED_QUEUE_NAME, CONNECTION_FAIL_MESSAGE,
                    new RabbitmqCommunicateException("Fail to connect. QueueName="
                            + WELL_DEFINED_QUEUE_NAME)));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        @SuppressWarnings("unchecked")
        @Before
        public void setUp() throws RabbitmqCommunicateException
        {
            AmqpTemplate mockTemplate = mock(AmqpTemplate.class);
            doThrow(AmqpException.class).when(mockTemplate).convertAndSend(CONNECTION_FAIL_MESSAGE);

            AmqpTemplateFactory mockFactory = mock(AmqpTemplateFactory.class);
            when(mockFactory.getAmqpTemplate(WELL_DEFINED_QUEUE_NAME)).thenReturn(mockTemplate);
            when(mockFactory.getAmqpTemplate(NOT_WELL_DEFINED_QUEUE_NAME)).thenThrow(
                    RabbitmqCommunicateException.class);

            this.client = new DefaultRabbitmqClient(mockFactory);
            this.client.setRetryInterval(RETRY_INTERVAL);
        }

        @Theory
        public void testSend_NormalCase(Fixture fixture) throws RabbitmqCommunicateException
        {
            // 前提
            Assume.assumeTrue(fixture.expectedSuccess());

            // 準備
            String queueName = fixture.queueName;
            Object message = fixture.message;

            // 実施
            this.client.send(queueName, message);

            // 検証
            verify(this.client.getTemplatefactory().getAmqpTemplate(queueName), times(1)).convertAndSend(
                    message);
        }

        @Theory
        public void testSend_ExceptionCase(Fixture fixture) throws RabbitmqCommunicateException
        {
            // 前提
            Assume.assumeTrue(fixture.expectedSuccess() == false);

            // 準備
            String queueName = fixture.queueName;
            Object message = fixture.message;
            expectedException.expect(fixture.expectedException.getClass());
            if (fixture.expectedException.getMessage() != null)
            {
                expectedException.expectMessage(fixture.expectedException.getMessage());
            }

            // 実施
            this.client.send(queueName, message);
        }

        @Test
        public void testSend_Retry() throws RabbitmqCommunicateException
        {
            // 準備
            String queueName = WELL_DEFINED_QUEUE_NAME;
            Object message = CONNECTION_FAIL_MESSAGE;

            // 実施
            try
            {
                this.client.send(queueName, message);
            }
            catch (Exception ex)
            {
                // リトライが行われていることを確認するため、例外は無視する。
            }

            // 検証
            AmqpTemplate mockAmqpTemplate = this.client.getTemplatefactory().getAmqpTemplate(
                    queueName);
            verify(mockAmqpTemplate, times(2)).convertAndSend(anyObject());
            verify(mockAmqpTemplate, times(2)).convertAndSend(message);
        }

        private static class Fixture
        {

            /** キュー名 */
            final String    queueName;

            /** メッセージ */
            final Object    message;

            /** 期待値(例外) */
            final Exception expectedException;

            /**
             * 正常系向けコンストラクタ
             * @param queueName
             * @param message
             * @param expectedException
             */
            public Fixture(String queueName, Object message)
            {
                this.queueName = queueName;
                this.message = message;
                this.expectedException = null;
            }

            /**
             * 異常系向けコンストラクタ
             * @param queueName
             * @param message
             * @param expectedException
             */
            public Fixture(String queueName, Object message, Exception expectedException)
            {
                super();
                this.queueName = queueName;
                this.message = message;
                this.expectedException = expectedException;
            }

            /**
             * 送信に成功することが期待されている場合、trueを返す。
             * @return 送信に成功することが期待されている場合、true
             */
            public boolean expectedSuccess()
            {
                return this.expectedException == null;
            }
        }
    }

    @RunWith(Theories.class)
    public static class DefaultRabbitmqClientTest_Receive
    {

        /** 例外出力確認用Ruleインスタンス */
        @Rule
        public ExpectedException      expectedException           = ExpectedException.none();

        /** 定義されているキュー名 */
        public final static String    WELL_DEFINED_QUEUE_NAME     = "snmpTrap_0";

        /** 定義されていないキュー名 */
        private static final String   NOT_WELL_DEFINED_QUEUE_NAME = "dummyQueue";

        /** 接続エラーが発生するキュー名 */
        private static final String   CONNECTION_FAIL_QUEUE_NAME  = "connection_fail_queue";

        /** 取得に成功した時のメッセージ */
        private static final Object   CONNECTION_SUCCESS_MESSAGE  = "success";

        /** RabbitMQクライアント */
        private DefaultRabbitmqClient client;

        /**
         * メッセージの受信
         * 
         * @target {@link DefaultRabbitmqClient#receive(String)}
         * @test メッセージの受信を確認する。(定義されているキュー名:受信)
         *    condition:: 定義されているキュー名
         *    result:: 受信できること
         * 
         * @test メッセージの受信を確認する。(定義されていないキュー名:例外出力)
         *    condition:: 定義されていないキュー名
         *    result:: 例外が出力されること
         * 
         * @test メッセージの受信を確認する。(定義されていないキュー名:例外メッセージ)
         *    condition:: 定義されていないキュー名
         *    result:: 例外メッセージが適切であること
         */
        @DataPoints
        public static Fixture[] createFixtures_QueueName()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // 定義されているキュー名
            patterns.add(new Fixture(WELL_DEFINED_QUEUE_NAME, CONNECTION_SUCCESS_MESSAGE));

            // 定義されていないキュー名
            patterns.add(new Fixture(NOT_WELL_DEFINED_QUEUE_NAME,
                    new RabbitmqCommunicateException()));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        /**
         * メッセージの受信
         * 
         * @target {@link DefaultRabbitmqClient#receive(String)}
         * @test メッセージの受信を確認する。(接続エラー:例外出力)
         *    condition:: 接続エラーが発生
         *    result:: 例外が出力されること
         * 
         * @test メッセージの受信を確認する。(接続エラー:例外メッセージ)
         *    condition:: 接続エラーが発生
         *    result:: 例外メッセージが適切であること
         */
        @DataPoints
        public static Fixture[] createFixture_ConnectionError()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // 接続エラー
            patterns.add(new Fixture(CONNECTION_FAIL_QUEUE_NAME, new RabbitmqCommunicateException(
                    "Fail to connect. QueueName=" + CONNECTION_FAIL_QUEUE_NAME)));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        @SuppressWarnings("unchecked")
        @Before
        public void setUp() throws RabbitmqCommunicateException
        {
            AmqpTemplate mockTemplate = mock(AmqpTemplate.class);
            when(mockTemplate.receiveAndConvert()).thenReturn(CONNECTION_SUCCESS_MESSAGE);

            AmqpTemplate mockConnectionFailTemplate = mock(AmqpTemplate.class);
            when(mockConnectionFailTemplate.receiveAndConvert()).thenThrow(AmqpException.class);

            AmqpTemplateFactory mockFactory = mock(AmqpTemplateFactory.class);
            when(mockFactory.getAmqpTemplate(NOT_WELL_DEFINED_QUEUE_NAME)).thenThrow(
                    RabbitmqCommunicateException.class);
            when(mockFactory.getAmqpTemplate(WELL_DEFINED_QUEUE_NAME)).thenReturn(mockTemplate);
            when(mockFactory.getAmqpTemplate(CONNECTION_FAIL_QUEUE_NAME)).thenReturn(
                    mockConnectionFailTemplate);

            this.client = new DefaultRabbitmqClient(mockFactory);
            this.client.setRetryInterval(RETRY_INTERVAL);
        }

        @Theory
        public void testReceive_NormalCase(Fixture fixture) throws RabbitmqCommunicateException
        {
            // 前提
            Assume.assumeTrue(fixture.expectedSuccess());

            // 準備
            String queueName = fixture.queueName;
            Object expect = fixture.expectedMessage;

            // 実施
            Object actual = this.client.receive(queueName);

            // 検証
            verify(this.client.getTemplatefactory().getAmqpTemplate(queueName), times(1)).receiveAndConvert();
            assertEquals(expect, actual);
        }

        @Theory
        public void testReceive_ExceptionCase(Fixture fixture) throws RabbitmqCommunicateException
        {
            // 前提
            Assume.assumeTrue(fixture.expectedSuccess() == false);

            // 準備
            String queueName = fixture.queueName;
            expectedException.expect(fixture.expectedException.getClass());
            if (fixture.expectedException.getMessage() != null)
            {
                expectedException.expectMessage(fixture.expectedException.getMessage());
            }

            // 実施
            this.client.receive(queueName);
        }

        @Test
        public void testReceive_Retry() throws RabbitmqCommunicateException
        {
            // 準備
            String queueName = CONNECTION_FAIL_QUEUE_NAME;

            // 実施
            try
            {
                this.client.receive(queueName);
            }
            catch (Exception ex)
            {
                // リトライが行われていることを確認するため、例外は無視する。
            }

            // 検証
            AmqpTemplate mockAmqpTemplate = this.client.getTemplatefactory().getAmqpTemplate(
                    queueName);
            verify(mockAmqpTemplate, times(2)).receiveAndConvert();
        }

        private static class Fixture
        {

            /** キュー名 */
            final String    queueName;

            /** 期待値(メッセージ) */
            final Object    expectedMessage;

            /** 期待値(例外) */
            final Exception expectedException;

            /**
             * 正常系向けコンストラクタ
             * @param queueName
             * @param expectedMessage
             */
            public Fixture(String queueName, Object expectedMessage)
            {
                this.queueName = queueName;
                this.expectedMessage = expectedMessage;
                this.expectedException = null;
            }

            /**
             * 異常系向けコンストラクタ
             * @param queueName
             * @param expectedException
             */
            public Fixture(String queueName, Exception expectedException)
            {
                this.queueName = queueName;
                this.expectedMessage = null;
                this.expectedException = expectedException;
            }

            /**
             * 送信に成功することが期待されている場合、trueを返す。
             * @return 送信に成功することが期待されている場合、true
             */
            public boolean expectedSuccess()
            {
                return this.expectedException == null;
            }
        }
    }

    @RunWith(Theories.class)
    public static class DefaultRabbitmqClientTest_QueueSize
    {

        /** 定義されているキュー名 */
        public final static String  WELL_DEFINED_QUEUE_NAME      = "snmpTrap_0";

        /** 定義されていないキュー名 */
        private static final String NOT_WELL_DEFINED_QUEUE_NAME  = "dummyQueue";

        /** 接続エラーが発生するキュー名 */
        private static final String CONNECTION_FAIL_QUEUE_NAME   = "connection_fail_queue";

        /** 取得に成功した時のメッセージ */
        private static final int    CONNECTION_SUCCESS_QUEUESIZE = 10;

        /** 例外出力確認用Ruleインスタンス */
        @Rule
        public ExpectedException    expectedException            = ExpectedException.none();

        /**
         * メッセージ件数の取得
         * 
         * @target {@link DefaultRabbitmqClient#getQueueSize(String)}
         * @test メッセージ件数の取得を確認する。(定義されているキュー名:メッセージ件数取得)
         *    condition:: 定義されているキュー名
         *    result:: メッセージ件数を取得できること
         * 
         * @test メッセージ件数の取得を確認する。(定義されていないキュー名:例外出力)
         *    condition:: 定義されていないキュー名
         *    result:: 例外が出力されること
         * 
         * @test メッセージ件数の取得を確認する。(定義されていないキュー名:例外メッセージ)
         *    condition:: 定義されていないキュー名
         *    result:: 例外メッセージが適切であること
         */
        @DataPoints
        public static Fixture[] createFixtures_QueueName()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // 定義されているキュー名
            patterns.add(new Fixture(WELL_DEFINED_QUEUE_NAME, CONNECTION_SUCCESS_QUEUESIZE));

            // 定義されていないキュー名
            patterns.add(new Fixture(NOT_WELL_DEFINED_QUEUE_NAME,
                    new RabbitmqCommunicateException()));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        /**
         * メッセージ件数の取得
         * 
         * @target {@link DefaultRabbitmqClient#getQueueSize(String)}
         * @test メッセージ件数の取得を確認する。(接続エラー:例外出力)
         *    condition:: 接続エラーが発生
         *    result:: 例外が出力されること
         * 
         * @test メッセージ件数の取得を確認する。(接続エラー:例外メッセージ)
         *    condition:: 接続エラーが発生
         *    result:: 例外メッセージが適切であること
         */
        @DataPoints
        public static Fixture[] createFixture_ConnectionError()
        {
            List<Fixture> patterns = new ArrayList<Fixture>();

            // 接続エラー
            patterns.add(new Fixture(CONNECTION_FAIL_QUEUE_NAME, new RabbitmqCommunicateException(
                    "QueueName is invalid. QueueName=" + CONNECTION_FAIL_QUEUE_NAME)));

            return patterns.toArray(new Fixture[patterns.size()]);
        }

        @SuppressWarnings("unchecked")
        @Before
        public void setUp() throws RabbitmqCommunicateException
        {
            RabbitTemplate mockTemplate = mock(RabbitTemplate.class);
            when(mockTemplate.execute(any(QueueSizeCallBack.class))).thenReturn(
                    CONNECTION_SUCCESS_QUEUESIZE);

            RabbitTemplate mockConnectionFailTemplate = mock(RabbitTemplate.class);
            when(mockConnectionFailTemplate.execute(any(QueueSizeCallBack.class))).thenThrow(
                    AmqpException.class);

            AmqpTemplateFactory mockFactory = mock(AmqpTemplateFactory.class);
            when(mockFactory.getAmqpTemplate(NOT_WELL_DEFINED_QUEUE_NAME)).thenThrow(
                    RabbitmqCommunicateException.class);
            when(mockFactory.getAmqpTemplate(WELL_DEFINED_QUEUE_NAME)).thenReturn(mockTemplate);
            when(mockFactory.getAmqpTemplate(CONNECTION_FAIL_QUEUE_NAME)).thenReturn(
                    mockConnectionFailTemplate);

            this.client = new DefaultRabbitmqClient(mockFactory);
            this.client.setRetryInterval(RETRY_INTERVAL);
        }

        /** RabbitMQクライアント */
        private DefaultRabbitmqClient client;

        @Theory
        public void testGetQueueSize_NormalCase(Fixture fixture)
                throws RabbitmqCommunicateException
        {
            // 前提
            Assume.assumeTrue(fixture.expectedSuccess());

            // 準備
            String queueName = fixture.queueName;
            int expect = fixture.expectedQueueSize;

            // 実施
            int actual = this.client.getQueueSize(queueName);

            // 検証
            verify((RabbitTemplate) this.client.getTemplatefactory().getAmqpTemplate(queueName),
                    times(1)).execute(any(QueueSizeCallBack.class));
            assertEquals(expect, actual);
        }

        @Theory
        public void testGetQueueSize_ExceptionCase(Fixture fixture)
                throws RabbitmqCommunicateException
        {
            // 前提
            Assume.assumeTrue(fixture.expectedSuccess() == false);

            // 準備
            String queueName = fixture.queueName;
            expectedException.expect(fixture.expectedException.getClass());
            if (fixture.expectedException.getMessage() != null)
            {
                expectedException.expectMessage(fixture.expectedException.getMessage());
            }

            // 実施
            this.client.getQueueSize(queueName);
        }

        @Test
        public void testGetQueueSize_Retry() throws RabbitmqCommunicateException
        {
            // 準備
            String queueName = CONNECTION_FAIL_QUEUE_NAME;

            // 実施
            try
            {
                this.client.getQueueSize(queueName);
            }
            catch (Exception ex)
            {
                // リトライが行われていることを確認するため、例外は無視する。
            }

            // 検証
            verify((RabbitTemplate) this.client.getTemplatefactory().getAmqpTemplate(queueName),
                    times(2)).execute(any(QueueSizeCallBack.class));
        }

        private static class Fixture
        {

            /** キュー名 */
            final String    queueName;

            /** 期待値(キューサイズ) */
            final int       expectedQueueSize;

            /** 期待値(例外) */
            final Exception expectedException;

            /**
             * 正常系向けコンストラクタ
             * @param queueName
             * @param expectedQueueSize
             */
            public Fixture(String queueName, int expectedQueueSize)
            {
                this(queueName, expectedQueueSize, null);
            }

            /**
             * 異常系向けコンストラクタ
             * @param queueName
             * @param expectedException
             */
            public Fixture(String queueName, Exception expectedException)
            {
                this(queueName, -1, expectedException);
            }

            private Fixture(String queueName, int expectedQueueSize, Exception expectedException)
            {
                this.queueName = queueName;
                this.expectedQueueSize = expectedQueueSize;
                this.expectedException = expectedException;
            }

            /**
             * 送信に成功することが期待されている場合、trueを返す。
             * @return 送信に成功することが期待されている場合、true
             */
            public boolean expectedSuccess()
            {
                return this.expectedException == null;
            }
        }
    }
}
