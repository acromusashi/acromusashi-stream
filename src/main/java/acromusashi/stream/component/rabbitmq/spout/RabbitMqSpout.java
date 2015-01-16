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
package acromusashi.stream.component.rabbitmq.spout;

import java.text.MessageFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.component.rabbitmq.RabbitmqClient;
import acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException;
import acromusashi.stream.constants.FieldName;
import acromusashi.stream.helper.SpringContextHelper;
import acromusashi.stream.spout.AmConfigurationSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * RabbitMQからメッセージを受信するSpoutクラス。
 *
 * @author kimura
 */
public class RabbitMqSpout extends AmConfigurationSpout
{
    /** serialVersionUID */
    private static final long          serialVersionUID = -7039267927348254032L;

    /** logger */
    private static final Logger        logger           = LoggerFactory.getLogger(RabbitMqSpout.class);

    /** RabbitMq通信クライアント */
    protected transient RabbitmqClient rabbitmqClient;

    /** キュー名称(ベース名称) */
    protected String                   queueName;

    /** キュー名称（実取得対象） */
    protected String                   targetQueueName;

    /** メッセージキー抽出用インターフェース */
    protected MessageKeyExtractor      messageKeyExtractor;

    /** RabbitMQ接続クライアント用コンテキストヘルパー */
    protected SpringContextHelper      contextHelper;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public RabbitMqSpout()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes"})
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        super.open(conf, context, collector);
        this.rabbitmqClient = this.contextHelper.getComponent(RabbitmqClient.class);
        // RabbitMQの取得対象キュー名称を「queueName」+「SpoutId(0オリジン)」で算出し、初期化
        this.targetQueueName = this.queueName + context.getThisTaskIndex();
    }

    @Override
    public void nextTuple()
    {
        Object receiveData = null;

        try
        {
            receiveData = this.rabbitmqClient.receive(this.targetQueueName);
        }
        catch (RabbitmqCommunicateException ex)
        {
            // メッセージ取得に失敗した場合、メッセージ取得を行わずに再度onNextTupleを実行させる。
            String messageFormat = "Message receive failed. QueueName={0}";
            String message = MessageFormat.format(messageFormat, this.targetQueueName);
            logger.warn(message, ex);
            return;
        }

        if (receiveData == null)
        {
            // メッセージを取得できなかった場合、メソッドを終了
            return;
        }

        String messageKey = null;

        try
        {
            messageKey = this.messageKeyExtractor.extractMessageKey(receiveData);
        }
        catch (RabbitmqCommunicateException ex)
        {
            // メッセージ抽出に失敗した場合、受信メッセージを破棄する。
            String messageFormat = "MessageKey extract failed. QueueName={0}, ReceiveData={1}";
            String message = MessageFormat.format(messageFormat, this.targetQueueName,
                    receiveData.toString());
            logger.warn(message, ex);
            return;
        }

        getCollector().emit(new Values(messageKey, receiveData.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
    }

    /**
     * @param queueName the queueName to set
     */
    public void setQueueName(String queueName)
    {
        this.queueName = queueName;
    }

    /**
     * @param messageKeyExtractor the messageKeyExtractor to set
     */
    public void setMessageKeyExtractor(MessageKeyExtractor messageKeyExtractor)
    {
        this.messageKeyExtractor = messageKeyExtractor;
    }

    /**
     * @param contextHelper the contextHelper to set
     */
    public void setContextHelper(SpringContextHelper contextHelper)
    {
        this.contextHelper = contextHelper;
    }
}
