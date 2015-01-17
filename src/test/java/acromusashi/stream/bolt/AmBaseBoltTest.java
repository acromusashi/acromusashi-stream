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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;

/**
 * AmBaseBoltクラスのテストクラス<br>
 * モッククラスを用いて検証を行う。
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class AmBaseBoltTest
{
    /** テスト対象 */
    private AmBaseBolt      target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map             mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext mockContext;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new AmBaseThroughBolt();
    }

    /**
     * Prepareメソッド呼び出し時の変数初期化確認を行う。
     *
     * @target {@link AmBaseBolt#prepare(Map, TopologyContext, OutputCollector)}
     * @test stormConf、TopologyContext、TaskIdが初期化されていることを確認
     *    condition:: prepareメソッド実行
     *    result:: stormConf、TopologyContext、TaskIdが初期化されていること
     */
    @Test
    public void testPrepare_変数初期化確認()
    {
        // 準備
        Mockito.when(this.mockContext.getThisComponentId()).thenReturn("ComponentId");
        Mockito.when(this.mockContext.getThisTaskIndex()).thenReturn(0);

        // 実施
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        // 検証
        assertThat(this.target.getStormConf(), sameInstance(this.mockConfMap));
        assertThat(this.target.getContext(), sameInstance(this.mockContext));
        assertThat(this.target.taskId, equalTo("ComponentId_0"));
    }

    /**
     * DeclareOutputFieldsメソッド呼び出し時のフィールド確認を行う。
     *
     * @target {@link AmBaseBolt#declareOutputFields(OutputFieldsDeclarer)}
     * @test メッセージキー、メッセージ値を指定して {@link OutputFieldsDeclarer#declare(Fields fields)}を呼び出していることを確認する。<br>
     *    condition:: declareOutputFieldsメソッド実行<br>
     *    result:: メッセージキー、メッセージ値を指定して {@link OutputFieldsDeclarer#declare(Fields fields)}を呼び出していること
     */
    @Test
    public void testDeclareOutputFields_フィールド確認()
    {
        // 準備
        OutputFieldsDeclarer mockDeclarer = Mockito.mock(OutputFieldsDeclarer.class);

        // 実施
        this.target.declareOutputFields(mockDeclarer);

        // 検証
        ArgumentCaptor<Fields> argument = ArgumentCaptor.forClass(Fields.class);
        Mockito.verify(mockDeclarer).declare(argument.capture());

        Fields argFields = argument.getValue();
        assertThat(argFields.size(), equalTo(2));
        assertThat(argFields.get(0), equalTo("messageKey"));
        assertThat(argFields.get(1), equalTo("messageValue"));
    }

    /**
     * KeyHistoryInfo未保持Tuple受信時、新規KeyHistoryInfoを生成して動作を継続することを確認する。
     *
     * @target {@link AmBaseBolt#execute(Tuple)}
     * @test 新規KeyHistoryInfoを生成して動作を継続することを確認
     *    condition:: KeyHistoryInfo未保持Tuple受信時
     *    result:: 新規KeyHistoryInfoを生成して動作を継続することを確認
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testExecute_KeyHistoryInfo未保持Tuple処理確認()
    {
        // 準備
        Mockito.when(this.mockContext.getThisComponentId()).thenReturn("ComponentId");
        Mockito.when(this.mockContext.getThisTaskIndex()).thenReturn(0);

        AmBaseThroughBolt targetBolt = new AmBaseThroughBolt();
        targetBolt.setFields(Lists.newArrayList("Param1"));
        targetBolt.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        Tuple mockTuple = Mockito.mock(Tuple.class);
        Mockito.doReturn("messageKey").when(mockTuple).getValueByField("messageKey");
        Mockito.doReturn(message).when(mockTuple).getValueByField("messageValue");
        Mockito.doReturn(new Fields("messageKey", "messageValue")).when(mockTuple).getFields();
        Mockito.doReturn(true).when(mockTuple).contains("messageValue");

        // 実施
        targetBolt.execute(mockTuple);

        // 検証
        ArgumentCaptor<Tuple> anchorCaptor = ArgumentCaptor.forClass(Tuple.class);
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(anchorCaptor.capture(), argument.capture());

        assertThat(anchorCaptor.getValue(), instanceOf(Tuple.class));
        assertThat(anchorCaptor.getValue(), sameInstance(mockTuple));

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);

        assertThat(sendMessage.getField("Param1").toString(), equalTo("Param1"));
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[KeyHistory]"));
    }

    /**
     * Tuple処理時、onExecuteメソッド実行中に例外が発生した場合、処理が終了することを確認する。
     *
     * @target {@link AmBaseBolt#execute(Tuple)}
     * @test 処理が終了することを確認
     *    condition:: onExecuteメソッド実行中に例外が発生した場合
     *    result:: 処理が終了することを確認
     */
    @Test(expected = RuntimeException.class)
    public void testExecute_OnExecute実行中例外発生()
    {
        // 準備
        Mockito.when(this.mockContext.getThisComponentId()).thenReturn("ComponentId");
        Mockito.when(this.mockContext.getThisTaskIndex()).thenReturn(0);

        AmBaseThroughBolt targetBolt = new AmBaseThroughBolt();
        targetBolt.setFields(Arrays.asList("Key", "Message"));
        targetBolt.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
        AmBaseThroughBolt mockedBolt = Mockito.spy(targetBolt);
        Mockito.doThrow(new RuntimeException()).when(mockedBolt).onExecute(any(StreamMessage.class));

        Tuple mockTuple = Mockito.mock(Tuple.class);
        Mockito.doReturn("Key").when(mockTuple).getValueByField("Key");
        Mockito.doReturn("Message").when(mockTuple).getValueByField("Message");

        // 実施
        mockedBolt.execute(mockTuple);

        // 検証
        fail("処理継続");
    }

    /**
     * Anchor、Keyを指定していない状態でemitメソッド呼び出し確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithNoAnchorKey(StreamMessage)}
     * @test 送信メッセージのKeyHistoryが空であることを確認
     *    condition:: Anchor、Keyを指定していない状態でメッセージ送信
     *    result:: 送信メッセージのKeyHistoryが空であること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_AnchorKey指定なし()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        AmBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        testTarget.emitWithNoAnchorKey(message);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));

        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);

        assertThat(sendMessage.getField("Param1").toString(), equalTo("Param1"));
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
    }

    /**
     * Keyを指定した状態でemitメソッド呼び出し確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithOnlyKey(StreamMessage, Object)}
     * @test 送信メッセージのKeyHistoryに指定したKey情報が追加されていること
     *    condition::  Keyを指定した状態でメッセージ送信
     *    result:: 送信メッセージのKeyHistoryに指定したKey情報が追加されていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_MessageKey指定()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        AmBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        testTarget.emitWithOnlyKey(message, "MessageKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));

        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);

        assertThat(sendMessage.getField("Param1").toString(), equalTo("Param1"));
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
    }

    /**
     * Anchorを指定した状態でのメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emitWithOnlyAnchor(StreamMessage)}
     * @test 送信メッセージのKeyHistoryが空であることを確認
     *    condition:: Anchorを指定した状態でメッセージ送信
     *    result:: 送信メッセージのKeyHistoryが空であること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Anchor指定()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
        Tuple mockTuple = Mockito.mock(Tuple.class);
        Whitebox.setInternalState(this.target, "executingTuple", mockTuple);

        AmBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        testTarget.emitWithOnlyAnchor(message);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq(mockTuple), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));

        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);

        assertThat(sendMessage.getField("Param1").toString(), equalTo("Param1"));
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
    }

    /**
     * Anchor、MessageKeyを指定した状態でメッセージ送信確認を行う。
     *
     * @target {@link AmBaseBolt#emit(StreamMessage, Object)}
     * @test 送信メッセージのKeyHistoryに指定したKey情報が追加されていること
     *    condition:: Anchor、MessageKeyを指定した状態でメッセージ送信
     *    result:: 送信メッセージのKeyHistoryに指定したKey情報が追加されていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_AnchorKey指定()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
        Tuple mockTuple = Mockito.mock(Tuple.class);
        Whitebox.setInternalState(this.target, "executingTuple", mockTuple);

        AmBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        testTarget.emit(message, "MessageKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq(mockTuple), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));

        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);

        assertThat(sendMessage.getField("Param1").toString(), equalTo("Param1"));
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
    }
}
