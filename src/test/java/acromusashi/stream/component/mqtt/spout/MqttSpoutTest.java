package acromusashi.stream.component.mqtt.spout;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.spout.SpoutOutputCollector;

/**
 * MqttSpoutのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class MqttSpoutTest
{
    /** テスト対象 */
    private MqttSpout                      target;

    /** テスト用のOutputCollector */
    @Mock
    private SpoutOutputCollector           mockCollector;

    /** MQTTConnection */
    @Mock
    private BlockingConnection             connection;

    /** MQTTMessage */
    @Mock
    private Message                        mqttMessage;

    /** Ack待ちのメッセージMap */
    private transient Map<String, Message> ackWaitMap;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        List<String> brokerAddresses = Lists.newArrayList();
        List<List<String>> subscribeTopics = Lists.newArrayList();
        this.target = new MqttSpout(brokerAddresses, subscribeTopics);

        this.ackWaitMap = Maps.newHashMap();

        Whitebox.setInternalState(this.target, "collector", mockCollector);
        Whitebox.setInternalState(this.target, "connection", connection);
        Whitebox.setInternalState(this.target, "ackWaitMap", ackWaitMap);
    }

    /**
     * メッセージの取得に失敗した場合、メッセージの送信が行われないことを確認する。
     *
     * @target {@link MqttSpout#nextTuple()}
     * @test メッセージの送信が行われないこと
     *    condition::  メッセージの取得に失敗
     *    result:: メッセージの送信が行われないこと
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testNextTuple_Message取得失敗() throws Exception
    {
        // 準備
        Mockito.when(this.connection.receive(10L, TimeUnit.SECONDS)).thenThrow(new Exception());

        // 実施
        this.target.nextTuple();

        // 検証
        Mockito.verify(this.mockCollector, Mockito.never()).emit(anyList(), anyObject());
        Mockito.verify(this.connection).resume();
    }

    /**
     * メッセージの取得結果がnullだった場合、メッセージの送信が行われないことを確認する。
     *
     * @target {@link MqttSpout#nextTuple()}
     * @test メッセージの送信が行われないこと
     *    condition::  メッセージの取得結果がnull
     *    result:: メッセージの送信が行われないこと
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testNextTuple_Message取得結果null() throws Exception
    {
        // 準備
        Mockito.when(this.connection.receive(10L, TimeUnit.SECONDS)).thenReturn(null);

        // 実施
        this.target.nextTuple();

        // 検証
        Mockito.verify(this.mockCollector, Mockito.never()).emit(anyList(), anyObject());
        Mockito.verify(this.connection, Mockito.never()).resume();
    }

    /**
     * 即Ackを返す設定でメッセージを受信した場合、Anchor無しでemitされることを確認する。
     *
     * @target {@link MqttSpout#nextTuple()}
     * @test Anchor無しでemitされること
     *    condition::  即Ackを返す設定でメッセージを受信
     *    result:: Anchor無しでemitされること
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNextTuple_Message取得成功即Ack() throws Exception
    {
        // 準備
        this.target.setImmidiateAck(true);
        Mockito.when(this.mqttMessage.getTopic()).thenReturn("TestTopic");
        Mockito.when(this.mqttMessage.getPayload()).thenReturn("TestMessage".getBytes("UTF-8"));
        Mockito.when(this.connection.receive(10L, TimeUnit.SECONDS)).thenReturn(this.mqttMessage);

        // 実施
        this.target.nextTuple();

        // 検証
        Mockito.verify(this.mqttMessage).ack();
        ArgumentCaptor<List> tupleCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleCaptor.capture());

        List tuple = tupleCaptor.getValue();
        assertThat(tuple.get(0).toString(), is("TestTopic"));
        acromusashi.stream.entity.StreamMessage message = (acromusashi.stream.entity.StreamMessage) tuple.get(1);
        assertThat(message.getHeader().getMessageKey(), is("TestTopic"));
        assertThat(message.getBody().toString(), is("TestMessage"));
    }

    /**
     * QoS0設定でメッセージを受信した場合、Anchor無しでemitされることを確認する。
     *
     * @target {@link MqttSpout#nextTuple()}
     * @test Anchor無しでemitされること
     *    condition::  QoS0設定でメッセージを受信
     *    result:: Anchor無しでemitされること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testNextTuple_Message取得成功QoS0() throws Exception
    {
        // 準備
        this.target.setQos(QoS.AT_MOST_ONCE);
        Mockito.when(this.mqttMessage.getTopic()).thenReturn("TestTopic");
        Mockito.when(this.mqttMessage.getPayload()).thenReturn("TestMessage".getBytes("UTF-8"));
        Mockito.when(this.connection.receive(10L, TimeUnit.SECONDS)).thenReturn(this.mqttMessage);

        // 実施
        this.target.nextTuple();

        // 検証
        Mockito.verify(this.mqttMessage).ack();
        ArgumentCaptor<List> tupleCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleCaptor.capture());

        List tuple = tupleCaptor.getValue();
        assertThat(tuple.get(0).toString(), is("TestTopic"));
        acromusashi.stream.entity.StreamMessage message = (acromusashi.stream.entity.StreamMessage) tuple.get(1);
        assertThat(message.getHeader().getMessageKey(), is("TestTopic"));
        assertThat(message.getBody().toString(), is("TestMessage"));
    }

    /**
     * QoS1設定でメッセージを受信した場合、Anchorつきでemitされることを確認する。
     *
     * @target {@link MqttSpout#nextTuple()}
     * @test Anchorつきでemitされること
     *    condition::  QoS1設定でメッセージを受信
     *    result:: Anchorつきでemitされること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testNextTuple_Message取得成功QoS1() throws Exception
    {
        // 準備
        Mockito.when(this.mqttMessage.getTopic()).thenReturn("TestTopic");
        Mockito.when(this.mqttMessage.getPayload()).thenReturn("TestMessage".getBytes("UTF-8"));
        Mockito.when(this.connection.receive(10L, TimeUnit.SECONDS)).thenReturn(this.mqttMessage);

        // 実施
        this.target.nextTuple();

        // 検証
        Mockito.verify(this.mqttMessage, Mockito.never()).ack();
        ArgumentCaptor<List> tupleCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Object> anchorCaptor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(this.mockCollector).emit(tupleCaptor.capture(), anchorCaptor.capture());

        List tuple = tupleCaptor.getValue();
        assertThat(tuple.get(0).toString(), is("TestTopic"));
        acromusashi.stream.entity.StreamMessage message = (acromusashi.stream.entity.StreamMessage) tuple.get(1);
        assertThat(message.getHeader().getMessageKey(), is("TestTopic"));
        assertThat(message.getBody().toString(), is("TestMessage"));

        Object anchor = anchorCaptor.getValue();
        assertThat(anchor, instanceOf(String.class));

        assertThat(this.ackWaitMap.values().contains(this.mqttMessage), is(true));

        // 実施2
        this.target.ack(anchor);
        Mockito.verify(this.mqttMessage).ack();
        assertThat(this.ackWaitMap.values().size(), is(0));
    }
}
