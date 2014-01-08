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

import java.text.MessageFormat;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * RabbitMQとの通信を行う。
 */
public class DefaultRabbitmqClient implements RabbitmqClient
{
    /** リトライするまでに待機する時間(デフォルト値) */
    private static final int    DEFAULT_RETRY_INTERVAL = 100;

    /** キューへのコネクション生成クラス */
    private AmqpTemplateFactory templatefactory;

    /** リトライするまでに待機する時間 */
    private int                 retryInterval          = DEFAULT_RETRY_INTERVAL;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public DefaultRabbitmqClient()
    {
        this(new AmqpTemplateFactory());
    }

    /**
     * デフォルトコンストラクタ
     * @param factory キューへのコネクション生成クラス
     */
    public DefaultRabbitmqClient(AmqpTemplateFactory factory)
    {
        this.templatefactory = factory;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void send(String queueName, Object message) throws RabbitmqCommunicateException
    {
        AmqpTemplate template = getTemplatefactory().getAmqpTemplate(queueName);
        try
        {
            sendAndRetry(template, message);
        }
        catch (Exception ex)
        {
            // リトライしても例外が出力された場合、例外を呼び出し元に返す
            String messageFmt = "Fail to connect. QueueName={0}";
            String errMessage = MessageFormat.format(messageFmt, queueName);
            throw new RabbitmqCommunicateException(errMessage, ex);
        }
    }

    /**
     * メッセージを送信する。
     * 
     * @param template キューへのコネクション
     * @param message メッセージ
     * @throws InterruptedException スレッド割り込みが発生した場合
     */
    private void sendAndRetry(AmqpTemplate template, Object message) throws InterruptedException
    {
        try
        {
            template.convertAndSend(message);
        }
        catch (AmqpException ex)
        {
            Thread.sleep(getRetryInterval());
            template.convertAndSend(message);
        }
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public Object receive(String queueName) throws RabbitmqCommunicateException
    {
        AmqpTemplate template = getTemplatefactory().getAmqpTemplate(queueName);
        Object message = null;
        try
        {
            message = receiveAndRetry(template);
        }
        catch (Exception ex)
        {
            String messageFmt = "Fail to connect. QueueName={0}";
            String errMessage = MessageFormat.format(messageFmt, queueName);
            throw new RabbitmqCommunicateException(errMessage, ex);
        }
        return message;
    }

    /**
     * メッセージを受信する。
     * 
     * @param template キューへのコネクション
     * @return メッセージ
     * @throws InterruptedException スレッド割り込みが発生した場合
     */
    private Object receiveAndRetry(AmqpTemplate template) throws InterruptedException
    {
        Object message = null;
        try
        {
            message = template.receiveAndConvert();
        }
        catch (AmqpException ex)
        {
            // 接続断のタイミングにより、例外が出力される場合があるため、1秒後にリトライする。
            // 再度発生した例外は呼び出し元に返す。
            Thread.sleep(getRetryInterval());
            message = template.receiveAndConvert();
        }
        return message;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getQueueSize(final String queueName) throws RabbitmqCommunicateException
    {
        RabbitTemplate template = (RabbitTemplate) getTemplatefactory().getAmqpTemplate(queueName);
        QueueSizeCallBack queueSizeCallBack = new QueueSizeCallBack(queueName);

        int size = 0;
        try
        {
            size = getQueueSizeAndRetry(template, queueSizeCallBack);
        }
        catch (Exception ex)
        {
            String messageFmt = "QueueName is invalid. QueueName={0}";
            String message = MessageFormat.format(messageFmt, queueName);
            throw new RabbitmqCommunicateException(message, ex);
        }
        return size;
    }

    /**
     * メッセージ件数を取得する。
     * 
     * @param template
     *            キューへのコネクション
     * @param queueSizeCallBack
     *            キューサイズを取得するコールバック
     * @return メッセージ件数
     * @throws InterruptedException
     *             スレッド割り込みが発生した場合
     */
    private int getQueueSizeAndRetry(RabbitTemplate template, QueueSizeCallBack queueSizeCallBack)
            throws InterruptedException
    {
        int size = 0;
        try
        {
            size = template.execute(queueSizeCallBack);
        }
        catch (AmqpException ex)
        {
            // 接続断のタイミングにより、例外が出力される場合があるため、間隔をあけてリトライする。
            // 再度発生した例外は呼び出し元に返す。
            Thread.sleep(getRetryInterval());
            size = template.execute(queueSizeCallBack);
        }
        return size;
    }

    /**
     * @return the templatefactory
     */
    public AmqpTemplateFactory getTemplatefactory()
    {
        return this.templatefactory;
    }

    /**
     * @return the retryInterval
     */
    public int getRetryInterval()
    {
        return this.retryInterval;
    }

    /**
     * リトライ間隔を検証して設定する。
     * 
     * @param retryInterval the retryInterval to set
     * @throws RabbitmqCommunicateException 指定したリトライ間隔が正の整数でない場合
     */
    public void setRetryInterval(int retryInterval) throws RabbitmqCommunicateException
    {
        if (retryInterval <= 0)
        {
            String messageFmt = "RetryInterval is invalid. RetryInterval={0}";
            String message = MessageFormat.format(messageFmt, retryInterval);
            throw new RabbitmqCommunicateException(message);
        }
        this.retryInterval = retryInterval;
    }
}
