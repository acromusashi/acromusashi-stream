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
package acromusashi.stream.component.mqtt.spout;

import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessageHeader;
import acromusashi.stream.spout.AmConfigurationSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

/**
 * MQTTコンポーネント<br>
 * <br>
 * MQTT Brokerからメッセージを受信するSpoutクラス。
 *
 * @author kimura
 */
public class MqttSpout extends AmConfigurationSpout
{
    /** serialVersionUID */
    private static final long              serialVersionUID     = 670832311695197611L;

    /** logger */
    private static final Logger            logger               = LoggerFactory.getLogger(MqttSpout.class);

    /** 受信QoSレベルのデフォルト値 */
    private static final QoS               DEFAULT_QOS          = QoS.AT_LEAST_ONCE;

    /** 受信タイムアウトのデフォルト値 */
    private static final long              DEFAULT_RECEIVE_WAIT = 10;

    /** 受信QoSレベル */
    private QoS                            qos                  = DEFAULT_QOS;

    /** メッセージ取得時、即Ackを返すか。QoSレベルが1 または2 の場合のみ有効 */
    private boolean                        immidiateAck         = false;

    /** MQTTBrokerアドレスリスト */
    private List<String>                   brokerUrls;

    /** 購読Topicリスト。Spoutのスレッド数分読みだされる。 */
    private List<List<String>>             subscribeTopics;

    /** 受信待ちタイムアウト(秒) */
    private long                           receiveWait          = DEFAULT_RECEIVE_WAIT;

    /** MQTTConnection */
    private transient BlockingConnection   connection;

    /** Ack待ちのメッセージMap */
    private transient Map<String, Message> ackWaitMap;

    /**
     * MQTTBrokerアドレスリスト、購読Topicリストを指定してSpoutを生成する。
     *
     * @param brokerAddresses MQTTBrokerアドレスリスト
     * @param subscribeTopics 購読Topicリスト
     */
    public MqttSpout(List<String> brokerAddresses, List<List<String>> subscribeTopics)
    {
        this.brokerUrls = brokerAddresses;
        this.subscribeTopics = subscribeTopics;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes"})
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        super.open(conf, context, collector);

        this.ackWaitMap = new HashMap<>();

        int taskIndex = context.getThisTaskIndex();
        String targetUrl = this.brokerUrls.get(taskIndex);
        List<String> targetTopicStrs = this.subscribeTopics.get(taskIndex);

        MQTT mqtt = new MQTT();
        try
        {
            mqtt.setHost(targetUrl);
            this.connection = mqtt.blockingConnection();
            this.connection.connect();

            String connectMsgFormat = "MQTT Broker connected. : Url={0}";
            logger.info(MessageFormat.format(connectMsgFormat, targetUrl));
            List<Topic> topicList = Lists.newArrayList();
            for (String topicStr : targetTopicStrs)
            {
                Topic topic = new Topic(topicStr, qos);
                topicList.add(topic);
            }
            Topic[] targetTopics = topicList.toArray(new Topic[0]);
            this.connection.subscribe(targetTopics);

            String subscribeMsgFormat = "MQTT Broker subscribed. : Topic={0}";
            logger.info(MessageFormat.format(subscribeMsgFormat, Arrays.toString(targetTopics)));
        }
        catch (Exception ex)
        {
            String msg = "MQTT Broker connect failed. Skip initialize Spout.";
            logger.error(msg, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nextTuple()
    {
        Message message = null;

        try
        {
            message = this.connection.receive(this.receiveWait, TimeUnit.SECONDS);
        }
        catch (Exception ex)
        {
            String msg = "Receive failed. Retry receive.";
            logger.warn(msg, ex);
            this.connection.resume();
        }

        // 取得できなかった場合は処理を終了する。
        if (message == null)
        {
            return;
        }

        String topic = message.getTopic();
        String payload = new String(message.getPayload(), Charset.forName("UTF-8"));
        acromusashi.stream.entity.StreamMessage sendMessage = createMessage(topic, payload);

        // 即応答を返す設定がされているか、またはQoSレベルが0の場合はAnchorを用いずに即Ackを返したうえでTupleを流す。
        // 上記以外の場合はランダムで生成したUUIDをAnchorとしてTupleを流す。
        if (this.immidiateAck == true || this.qos == QoS.AT_MOST_ONCE)
        {
            message.ack();
            getCollector().emit(new Values(topic, sendMessage));
        }
        else
        {
            String messageId = UUID.randomUUID().toString();
            this.ackWaitMap.put(messageId, message);
            getCollector().emit(new Values(topic, sendMessage), messageId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(Object msgId)
    {
        Message message = this.ackWaitMap.remove(msgId);
        if (message != null)
        {
            message.ack();
        }
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
     * AcroMUSASHI用メッセージを生成する。
     *
     * @param topic Topic
     * @param payload Payload
     * @return AcroMUSASHI用メッセージ
     */
    protected acromusashi.stream.entity.StreamMessage createMessage(String topic, String payload)
    {
        StreamMessageHeader header = new StreamMessageHeader();
        header.setTimestamp(getCurrentTime());
        header.setType("MQTT");
        header.setMessageKey(topic);

        acromusashi.stream.entity.StreamMessage message = new acromusashi.stream.entity.StreamMessage(header,
                payload);
        return message;
    }

    /**
     * 現在時刻を算出する。
     *
     * @return 現在時刻
     */
    protected long getCurrentTime()
    {
        return System.currentTimeMillis();
    }

    /**
     * @param receiveWait セットする receiveWait
     */
    public void setReceiveWait(long receiveWait)
    {
        this.receiveWait = receiveWait;
    }

    /**
     * @param qos セットする qos
     */
    public void setQos(QoS qos)
    {
        this.qos = qos;
    }

    /**
     * @param immidiateAck セットする immidiateAck
     */
    public void setImmidiateAck(boolean immidiateAck)
    {
        this.immidiateAck = immidiateAck;
    }
}
