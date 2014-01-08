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

import org.springframework.amqp.rabbit.core.ChannelCallback;

import com.rabbitmq.client.Channel;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * キューサイズを取得する。
 * 
 * @author otoda
 */
public class QueueSizeCallBack implements ChannelCallback<Integer>
{
    /** キュー名 */
    private String queueName;

    /**
     * キュー名を指定してインスタンスを生成する。
     * 
     * @param queueName キュー名
     */
    public QueueSizeCallBack(String queueName)
    {
        this.queueName = queueName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer doInRabbit(Channel channel) throws Exception
    {
        return channel.queueDeclarePassive(this.queueName).getMessageCount();
    }
}
