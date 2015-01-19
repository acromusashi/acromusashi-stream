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
package acromusashi.stream.bolt.jdbc;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.AmConfigurationBolt;
import acromusashi.stream.camel.CamelInitializer;
import acromusashi.stream.converter.AbstractMessageConverter;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.ConvertFailException;
import acromusashi.stream.exception.InitFailException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

import com.google.common.collect.Lists;

/**
 * Camel-SQL Componentを利用して、受信したMessageをDBに保存するBolt。<br/>
 * 指定したテーブルに対して下記の順にカラムにマッピングさせ、データを投入する。<br/>
 *
 * <ol>
 * <li>Message:HeaderのMessageId</li>
 * <li>Message:Headerのtimestamp</li>
 * <li>Message:Headerのsource</li>
 * <li>Message:Body1要素目</li>
 * <li>Message:Body2要素目....(以後、Bodyの要素がなくなるまで追加)</li>
 * </ol>
 *
 * @author tsukano
 */
public class CamelJdbcStoreBolt extends AmConfigurationBolt
{
    /** serialVersionUID */
    private static final long          serialVersionUID = -668373233969623288L;

    /** logger */
    private static final Logger        logger           = LoggerFactory.getLogger(CamelJdbcStoreBolt.class);

    /** Camelで使用するendpointUri。デフォルト値は"direct:CamelSqlBolt" */
    private String                     endpointUri      = "direct:CamelJdbcBolt";

    /** ApplicationContextのファイルパス。デフォルト値は"camel-context_jdbc.xml" */
    private String                     contextUri       = "camel-context_jdbc.xml";

    /** メッセージからレコードを生成するコンバータクラス */
    protected AbstractMessageConverter converter;

    /** Camelにオブジェクトを送信するクラス */
    private transient ProducerTemplate producerTemplate;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public CamelJdbcStoreBolt()
    {}

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);

        try
        {
            this.producerTemplate = CamelInitializer.generateTemplete(this.contextUri);
        }
        catch (Exception ex)
        {
            logger.error("Failure to get ProducerTemplate.", ex);
            throw new InitFailException(ex);
        }
    }

    /**
     * Message受信時の処理を行う
     *
     * @param message 受信Message
     */
    @Override
    @SuppressWarnings("unchecked")
    public void onMessage(StreamMessage message)
    {
        String endopointUri = getEndpointUri();

        Map<String, Object> resultMap;

        try
        {
            resultMap = this.converter.toMap(message);
        }
        catch (ConvertFailException ex)
        {
            String logFormat = "Fail convert to jdbc record. Dispose received message. : Message={0}";
            logger.warn(MessageFormat.format(logFormat, message), ex);
            ack();
            return;
        }

        List<Object> valueList = Lists.newArrayList();

        valueList.add(resultMap.get("messageId"));
        valueList.add(resultMap.get("timestamp"));
        valueList.add(resultMap.get("source"));

        List<String> bodyList = (List<String>) resultMap.get("body");

        valueList.addAll(bodyList);

        insert(endopointUri, valueList);
        ack();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // This class not has downstream component.
    }

    /**
     * endpointUriを指定して、DBにinsertを行う。
     * @param endpointUri CamelのendpointUri
     * @param values PreparedStatementに設定する値
     */
    protected void insert(String endpointUri, Collection<Object> values)
    {
        this.producerTemplate.sendBody(endpointUri, values);
    }

    /**
     * ApplicationContextUriを指定する。
     * このメソッドは{@link #prepare(Map, TopologyContext, OutputCollector)}が呼び出されるより前に呼び出すこと。
     * したがって、Topologyをsubmitするより前に呼び出すこと。
     * ApplicationContextUriに指定したファイルは{@link #prepare(Map, TopologyContext, OutputCollector)}で読み込む。
     * @param applicationContextUri ApplicationContextUri
     */
    public void setApplicationContextUri(String applicationContextUri)
    {
        this.contextUri = applicationContextUri;
    }

    /**
     * endpointUriを返す。
     * @return endpointUri
     */
    protected String getEndpointUri()
    {
        return this.endpointUri;
    }

    /**
     * endpointUriを設定する。
     *
     * @param endpointUri endpointUri
     */
    public void setEndpointUri(String endpointUri)
    {
        this.endpointUri = endpointUri;
    }

    /**
     * @param converter セットする converter
     */
    public void setConverter(AbstractMessageConverter converter)
    {
        this.converter = converter;
    }
}
