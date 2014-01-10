package acromusashi.stream.component.rabbitmq;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

public class QueueSizeCallBackTest {

    /** 例外出力確認用Ruleインスタンス */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String queueName = "queue";

    /**
     * キューサイズの取得
     * 
     * @target {@link QueueSizeCallBack#doInRabbit(Channel)}
     * @test キューサイズの取得を確認する。(存在しないキュー名：例外出力)
     *    condition:: キュー名:存在しないキュー名
     *    result:: 例外が出力されること
     */
    @Test
    public void testDoInRabbit_存在しないキュー() throws Exception {
        // 準備
        QueueSizeCallBack callBack = new QueueSizeCallBack(queueName);

        Channel mockChannel = mock(Channel.class);
        IOException toBeThrown = new IOException();
        doThrow(toBeThrown).when(mockChannel).queueDeclarePassive(anyString());

        this.expectedException.expect(is(toBeThrown));

        // 実施
        callBack.doInRabbit(mockChannel);
    }

    /**
     * キューサイズの取得
     * 
     * @target {@link QueueSizeCallBack#doInRabbit(Channel)}
     * @test キューサイズの取得を確認する。(存在するキュー名：キューサイズの取得)
     *    condition:: キュー名:存在するキュー名
     *    result:: キューサイズを取得できること
     */
    @Test
    public void testDoInRabbit_正常系() throws Exception {
        // 準備
        QueueSizeCallBack callBack = new QueueSizeCallBack(queueName);

        int expect = 10;
        Channel mockChannel = mock(Channel.class);
        DeclareOk mockQueue = mock(DeclareOk.class);
        doReturn(mockQueue).when(mockChannel).queueDeclarePassive(anyString());
        doReturn(expect).when(mockQueue).getMessageCount();

        // 実施
        int actual = callBack.doInRabbit(mockChannel);

        // 検証
        assertThat(actual, is(expect));
    }
}
