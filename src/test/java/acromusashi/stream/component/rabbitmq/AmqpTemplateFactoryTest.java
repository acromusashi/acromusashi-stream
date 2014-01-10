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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@RunWith(Theories.class)
public class AmqpTemplateFactoryTest
{

    /** 定義されたキュー名 */
    private static final String DEFINED_QUEUE_NAME     = "definedQueueName";

    /** 定義されていないキュー名 */
    private static final String NON_DEFINED_QUEUE_NAME = "nonDefinedQueue";

    /** 例外出力確認用Ruleインスタンス */
    @Rule
    public ExpectedException    expectedException      = ExpectedException.none();

    /** キューへのコネクションを保持する */
    private AmqpTemplateFactory factory;

    /**
     * コネクションの生成
     * 
     * @target {@link AmqpTemplateFactory#getAmqpTemplate(String)}
     * @test コネクションの生成(キュー名null:例外出力)
     *    condition:: キュー名:null
     *    result:: 例外が出力されること
     * 
     * @test コネクションの生成(キュー名null:例外メッセージ)
     *    condition:: キュー名:null
     *    result:: 例外メッセージが適切であること
     * 
     * @test コネクションの生成(キュー名空文字:例外出力)
     *    condition:: キュー名:空文字
     *    result:: 例外が出力されること
     * 
     * @test コネクションの生成(キュー名空文字:例外メッセージ)
     *    condition:: キュー名:空文字
     *    result:: 例外メッセージが適切であること
     * 
     * @test コネクションの生成(定義されたキュー名:コネクション生成)
     *    condition:: キュー名:定義されたキュー名
     *    result:: コネクションが生成されること
     * 
     * @test コネクションの生成(定義されたキュー名:ConnectionFactory生成)
     *    condition:: キュー名:定義されたキュー名
     *    result:: 設定したConnectionFactoryが使用されていること
     * 
     * @test コネクションの生成(定義されたキュー名:キャッシュの使用)
     *    condition:: キュー名:定義されたキュー名
     *    result:: 2回目取得時に、1回目に生成したコネクションを返却すること
     * 
     * @test コネクションの生成(定義されていないキュー名:例外出力)
     *    condition:: キュー名:定義されていないキュー名
     *    result:: 例外が出力されること
     * 
     * @test コネクションの生成(定義されていないキュー名:例外メッセージ)
     *    condition:: キュー名:定義されていないキュー名
     *    result:: 例外メッセージが適切であること
     */
    @DataPoints
    public static Fixture[] createFixture_QueueName()
    {
        List<Fixture> patterns = new ArrayList<Fixture>();

        // null
        patterns.add(new Fixture(null, new RabbitmqCommunicateException(
                "QueueName is not defined.")));

        // 空文字
        patterns.add(new Fixture("", new RabbitmqCommunicateException(
                "QueueNames ProcessList is not defined. QueueName={0}")));

        // 定義されたキュー名
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setChannelCacheSize(10);
        patterns.add(new Fixture(DEFINED_QUEUE_NAME, new RabbitTemplate(connectionFactory)));

        // 定義されていないキュー名
        patterns.add(new Fixture(NON_DEFINED_QUEUE_NAME, new RabbitmqCommunicateException(
                "QueueNames ProcessList is not defined. QueueName={0}")));

        return patterns.toArray(new Fixture[patterns.size()]);
    }

    @Theory
    public void testGetAmqpTemplate_NormalCase(Fixture fixture) throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.expectedException == null);

        // 準備
        String queueName = fixture.queueName;
        RabbitTemplate expected = fixture.expectedTemplate;

        // Mock設定
        AbstractContextBuilder mockContextBuilder = mock(AbstractContextBuilder.class);
        doReturn(Arrays.asList("localhost:5672", "172.0.0.1:5673")).when(mockContextBuilder).getProcessList(
                DEFINED_QUEUE_NAME);
        doReturn(expected.getConnectionFactory()).when(mockContextBuilder).getConnectionFactory(
                DEFINED_QUEUE_NAME);
        doReturn(expected).when(mockContextBuilder).getAmqpTemplate(DEFINED_QUEUE_NAME);

        this.factory = new AmqpTemplateFactory(mockContextBuilder);

        // 実施
        RabbitTemplate firstActual = (RabbitTemplate) this.factory.getAmqpTemplate(queueName);
        RabbitTemplate secondActual = (RabbitTemplate) this.factory.getAmqpTemplate(queueName);

        // 検証
        // AmqpTemplateもConnectionFactoryと同様、equalsで比較できないため、
        // 生成のためにContextBuilderから取得していることを確認する
        verify(mockContextBuilder, times(1)).getAmqpTemplate(DEFINED_QUEUE_NAME);

        // ConnectionFactoryが同一のものであるかどうかはインスタンスのフィールド(キャッシュサイズ)で比較する。
        // ディープコピーされるため、インスタンスが異なり、equalsで比較できないため。
        assertThat(
                ((CachingConnectionFactory) firstActual.getConnectionFactory()).getChannelCacheSize(),
                is(((CachingConnectionFactory) expected.getConnectionFactory()).getChannelCacheSize()));

        // 保持していたコネクション(同一インスタンス)を返していること
        assertTrue(firstActual == secondActual);
    }

    @Theory
    public void testGetAmqpTemplate_ExceptionCase(Fixture fixture)
            throws RabbitmqCommunicateException
    {
        // 前提
        Assume.assumeTrue(fixture.expectedException != null);

        // 準備
        String queueName = fixture.queueName;
        expectedException.expect(fixture.expectedException.getClass());
        if (fixture.expectedException.getMessage() != null)
        {
            expectedException.expectMessage(is(fixture.expectedException.getMessage()));
        }

        AbstractContextBuilder mockContextBuilder = mock(AbstractContextBuilder.class);
        doReturn(Arrays.asList("localhost:5672", "172.0.0.1:5673")).when(mockContextBuilder).getProcessList(
                DEFINED_QUEUE_NAME);

        this.factory = new AmqpTemplateFactory(mockContextBuilder);

        // 実施
        this.factory.getAmqpTemplate(queueName);
    }

    private static class Fixture
    {

        /** キュー名 */
        String          queueName;

        /** 期待値(キューへのコネクション) */
        RabbitTemplate  expectedTemplate;

        /** 期待値(例外) */
        final Exception expectedException;

        /**
         * @param queueName
         * @param expectedTemplate
         */
        public Fixture(String queueName, RabbitTemplate expectedTemplate)
        {
            this.queueName = queueName;
            this.expectedTemplate = expectedTemplate;
            this.expectedException = null;
        }

        /**
         * @param queueName
         * @param expectedTemplate
         * @param expectedException
         */
        public Fixture(String queueName, Exception expectedException)
        {
            this.queueName = queueName;
            this.expectedTemplate = null;
            this.expectedException = expectedException;
        }
    }
}
