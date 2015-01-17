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
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.converter.AbstractMessageConverter;
import acromusashi.stream.entity.StreamMessageHeader;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * メッセージ変換を行うBolt<br/>
 * 受信した文字列を設定されたConverterを用いてMessageに変換を行う。<br/>
 * KestrelThriftSpoutから文字列を受信するため、Tuple中のフィールド"str"を用いる。<br/>
 * 
 * @author kimura
 */
public class MessageConvertBolt extends AmConfigurationBolt
{
    /** serialVersionUID */
    private static final long          serialVersionUID = 8285275433076201532L;

    /** logger */
    private static final Logger        logger           = LoggerFactory.getLogger(MessageConvertBolt.class);

    /** Message生成用のコンバータ */
    protected AbstractMessageConverter converter;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public MessageConvertBolt()
    {}

    @Override
    public void execute(Tuple input)
    {
        if (logger.isDebugEnabled() == true)
        {
            String logFormat = "Received tuple. : ReceivedTuple={0}";
            String logMessage = MessageFormat.format(logFormat, input);
            logger.debug(logMessage);
        }

        // Get 'str' field. from KestrelThriftSpout.
        Object obj = input.getValueByField("str");
        if (obj == null || !(obj instanceof String))
        {
            String logFormat = "Failed to get String. Dispose tuple. : ReceivedTuple={0}";
            String logMessage = MessageFormat.format(logFormat, input);
            logger.warn(logMessage);
            getCollector().ack(input);
            return;
        }

        try
        {
            StreamMessageHeader header = this.converter.createHeader(obj);

            if (StringUtils.isEmpty(header.getMessageId()) == true)
            {
                header.setMessageId(UUID.randomUUID().toString());
            }

            if (header.getTimestamp() == 0)
            {
                header.setTimestamp(System.currentTimeMillis());
            }

            Object body = this.converter.createBody(obj);
            StreamMessage message = new StreamMessage();
            message.setHeader(header);
            message.setBody(body);

            getCollector().emit(new Values(message));
            getCollector().ack(input);
        }
        catch (Exception ex)
        {
            String logFormat = "Failed to message conversion. Dispose tuple. : ReceivedTuple={0}";
            String logMessage = MessageFormat.format(logFormat, input);
            logger.warn(logMessage, ex);
            getCollector().ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(FieldName.MESSAGE_VALUE));
    }

    /**
     * @param converter the converter to set
     */
    public void setConverter(AbstractMessageConverter converter)
    {
        this.converter = converter;
    }
}
