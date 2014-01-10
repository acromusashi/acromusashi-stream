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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.Message;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * メッセージ変換を行うBolt<br/>
 * 受信した文字列を設定されたConverterを用いてMessageに変換を行う。<br/>
 * 
 * @author kimura
 */
public class JsonConvertBolt<T> extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long        serialVersionUID = 4002032169715662295L;

    /** logger */
    private static final Logger      logger           = LoggerFactory.getLogger(JsonConvertBolt.class);

    /** 変換対象のエンティティクラス */
    protected Class<T>               userObjectClass;

    /** Jacksonを用いた変換マッパーオブジェクト */
    protected transient ObjectMapper objectMapper;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public JsonConvertBolt()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);

        // 変換マッパー初期化
        this.objectMapper = new ObjectMapper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        String keyStr = input.getStringByField(FieldName.MESSAGE_KEY);
        String messageStr = input.getStringByField(FieldName.MESSAGE_VALUE);

        Message parentEntity = null;

        try
        {
            // JSONから共通メッセージエンティティに変換する
            parentEntity = this.objectMapper.readValue(messageStr.toString(), Message.class);
        }
        catch (IOException ex)
        {
            // 変換に失敗した場合はメッセージを破棄する。
            // failは返さず、ackを返す。
            // ackを返す理由はfailを返した場合Spoutにfailが返り、メッセージの再取得⇒変換失敗を繰り返してしまうため。
            logger.warn(
                    "Failed convert to CommonMessageEntity. Dispose message. Tuple="
                            + input.toString(), ex);
            getCollector().ack(input);
            return;
        }

        T userObject = null;

        try
        {
            // ユーザオブジェクト部のJSONを取得する。
            String userObjectStr = this.objectMapper.writeValueAsString(parentEntity.getBody());
            // JSONからユーザオブジェクトに変換する。
            userObject = this.objectMapper.readValue(userObjectStr, this.userObjectClass);
            parentEntity.setBody(userObject);
        }
        catch (IOException ex)
        {
            // 変換に失敗した場合はメッセージを破棄する。
            // failは返さず、ackを返す。
            // ackを返す理由はfailを返した場合Spoutにfailが返り、メッセージの再取得⇒変換失敗を繰り返してしまうため。
            logger.warn("Failed convert to UserEntity. Dispose message. Tuple=" + input.toString(),
                    ex);
            getCollector().ack(input);
            return;
        }

        getCollector().emit(input, new Values(keyStr, parentEntity));
        getCollector().ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
    }

    /**
     * @param userObjectClass the userObjectClass to set
     */
    public void setUserObjectClass(Class<T> userObjectClass)
    {
        this.userObjectClass = userObjectClass;
    }
}
