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

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessageHeader;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 受信したTupleの内容をログ出力するBolt
 * 
 * @author kimura
 */
public class MessagePrintBolt extends AmConfigurationBolt
{
    /** serialVersionUID */
    private static final long   serialVersionUID = -6390790906598881431L;

    /** logger */
    private static final Logger logger           = LoggerFactory.getLogger(MessageConvertBolt.class);

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public MessagePrintBolt()
    {}

    @Override
    public void execute(Tuple input)
    {
        StreamMessage message = (StreamMessage) input.getValueByField(FieldName.MESSAGE_VALUE);

        StreamMessageHeader header = message.getHeader();
        Object body = message.getBody();

        logger.info("ReceiveHeader=" + header.toString() + " ,ReceiveBody="
                + ToStringBuilder.reflectionToString(body, ToStringStyle.SHORT_PREFIX_STYLE));

        getCollector().ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // Do nothing.
    }

}
