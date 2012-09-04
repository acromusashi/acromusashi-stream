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

import java.text.MessageFormat;

import org.apache.log4j.Logger;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.converter.AbstractMessageConverter;
import acromusashi.stream.entity.Message;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Messageを受信して処理するBoltの基底クラス<br/>
 * 受信したTupleからMessageオブジェクトを取得し、onMessageメソッドを呼び出す。<br/>
 * Message処理を行うBoltを作成する場合、本クラスを継承してonMessageメソッドの実装を行うこと。
 * 
 * @author kimura
 */
public abstract class MessageBolt extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long          serialVersionUID = -434252908894821429L;

    /** logger */
    private static final Logger        logger           = Logger.getLogger(MessageBolt.class);

    /** Message生成用のコンバータ */
    protected AbstractMessageConverter converter;

    @Override
    public void execute(Tuple input)
    {
        if (logger.isDebugEnabled() == true)
        {
            String logFormat = "Received tuple. : ReceivedTuple={0}";
            String logMessage = MessageFormat.format(logFormat, input);
            logger.debug(logMessage);
        }

        // Tupleのフィールド"message"にMessageが設定されていない場合はnullを返す
        Object obj = input.getValueByField(FieldName.MESSAGE);
        if (obj == null || !(obj instanceof Message))
        {
            String logFormat = "Failed to get message object. Skip message prosessing. : InputTuple={0}";
            String logMessage = MessageFormat.format(logFormat, input);
            logger.warn(logMessage);
            getCollector().ack(input);
            return;
        }

        Message message = (Message) obj;

        try
        {
            onMessage(message);
        }
        catch (Exception ex)
        {
            String logFormat = "Failed to message prosessing. Dispose message. : Message={0}";
            String logMessage = MessageFormat.format(logFormat, message);
            logger.warn(logMessage, ex);
            getCollector().fail(input);
            return;
        }

        getCollector().ack(input);
    }

    /**
     * Message受信時の処理を行う
     * 
     * @param message 受信Message
     * @throws Exception 受信処理失敗時 
     */
    public abstract void onMessage(Message message) throws Exception;

    /**
     * @param converter the converter to set
     */
    public void setConverter(AbstractMessageConverter converter)
    {
        this.converter = converter;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("message"));
    }
}
