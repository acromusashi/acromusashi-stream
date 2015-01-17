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
package acromusashi.stream.spout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.entity.StreamMessage;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * KeyTraceBaseSpoutクラスのテストクラス<br>
 * モッククラスを用いて検証を行う。
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class AmBaseSpoutTest
{
    /** テスト対象 */
    private AmBaseSpout          target;

    /** テスト用のOutputCollector */
    @Mock
    private SpoutOutputCollector mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map                  mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext      mockContext;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new BlankAmBaseSpout();
    }

    /**
     * Openメソッド呼び出し時の変数初期化確認を行う。
     *
     * @target {@link AmBaseSpout#open(Map, TopologyContext, SpoutOutputCollector)}
     * @test stormConf、TopologyContext、TaskIdが初期化されていることを確認<br>
     *    condition:: openメソッド実行<br>
     *    result:: stormConf、TopologyContext、TaskIdが初期化されていること
     */
    @Test
    public void testOpen_変数初期化確認()
    {
        // 準備
        Mockito.when(this.mockContext.getThisComponentId()).thenReturn("ComponentId");
        Mockito.when(this.mockContext.getThisTaskIndex()).thenReturn(0);

        // 実施
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        // 検証
        assertThat(this.target.getStormConf(), sameInstance(this.mockConfMap));
        assertThat(this.target.getContext(), sameInstance(this.mockContext));
        assertThat(this.target.taskId, equalTo("ComponentId_0"));
    }

    /**
     * DeclareOutputFieldsメソッド呼び出し時のフィールド確認を行う。
     *
     * @target {@link AmBaseSpout#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)}
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
     * Emitメソッド呼び出し時（KeyId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emit(StreamMessage, Object)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);
        this.target.setRecordHistory(false);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emit(message, "MessageKeyId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageKeyId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId指定、履歴記録オフ）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emit(StreamMessage, Object)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId指定_履歴記録オフ()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emit(message, "MessageKeyId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageKeyId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKeyId]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId指定、GroupingKey指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithGrouping(StreamMessage, Object, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageId、GroupingKeyを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId指定_Grouping()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithGrouping(message, "MessageKeyId", "GroupingKey");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageKeyId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKeyId]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId指定、StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithStream(StreamMessage, Object, String)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId指定_Stream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithStream(message, "MessageKeyId", "StreamId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), tupleArgument.capture(),
                eq("MessageKeyId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKeyId]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId指定、GroupingKey&StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithGroupingStream(StreamMessage, Object, String, String)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId指定_GroupingStream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithGroupingStream(message, "MessageKeyId", "GroupingKey", "StreamId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), tupleArgument.capture(),
                eq("MessageKeyId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKeyId]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId未指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithNoKeyId(StreamMessage)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定せずにTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testEmit_KeyId未指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithNoKeyId(message);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId未指定、GroupingKey指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithNoKeyIdAndGrouping(StreamMessage, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定せずにTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testEmit_KeyId未指定_Grouping()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithNoKeyIdAndGrouping(message, "GroupingKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId未指定、StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithNoKeyIdAndStream(StreamMessage, String)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定せずにTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testEmit_KeyId未指定_Stream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithNoKeyIdAndStream(message, "StreamId");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId未指定、GroupingKey&StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithNoKeyIdAndGroupingStream(StreamMessage, String, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを指定せずにTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testEmit_KeyId未指定_GroupingStream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithNoKeyIdAndGroupingStream(message, "GroupingKey", "StreamId");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory(), nullValue());
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（Key指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKey(StreamMessage, Object)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKeyを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Key指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKey(message, "MessageKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（Key指定、GroupingKey指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyAndGrouping(StreamMessage, Object, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKeyを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Key指定_Grouping()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyAndGrouping(message, "MessageKey", "GroupingKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（Key指定、StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyAndStream(StreamMessage, Object, String)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKeyを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Key指定_Stream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyAndStream(message, "MessageKey", "StreamId");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（Key指定、GroupingKey&StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyAndGroupingStream(StreamMessage, Object, String, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKeyを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Key指定_GroupingStream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyAndGroupingStream(message, "MessageKey", "GroupingKey", "StreamId");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId個別指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyId(StreamMessage, Object, Object)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを個別指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId個別指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyId(message, "MessageKey", "MessageId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId個別指定、GroupingKey指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyIdAndGrouping(StreamMessage, Object, Object, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを個別指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId個別指定_Grouping()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyIdAndGrouping(message, "MessageKey", "MessageId", "GroupingKey");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId個別指定、StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyIdAndStream(StreamMessage, Object, Object, String)}
     * @test collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを個別指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey(空文字)、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId個別指定_Stream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyIdAndStream(message, "MessageKey", "MessageId", "StreamId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), tupleArgument.capture(),
                eq("MessageId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo(""));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }

    /**
     * Emitメソッド呼び出し時（KeyId個別指定、GroupingKey&StreamId指定）の引数確認を行う。
     *
     * @target {@link AmBaseSpout#emitWithKeyIdAndGroupingStream(StreamMessage, Object, Object, String, String)}
     * @test collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること。<br>
     *    condition:: MessageKey、MessageIdを個別指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleはGroupingKey、StreamMessageの順となっていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId個別指定_GroupingStream()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        StreamMessage message = new StreamMessage();
        message.addField("Param1", "Param1");

        // 実施
        this.target.emitWithKeyIdAndGroupingStream(message, "MessageKey", "MessageId",
                "GroupingKey", "StreamId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(eq("StreamId"), tupleArgument.capture(),
                eq("MessageId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo("GroupingKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getHeader().getHistory().toString(),
                equalTo("KeyHistory=[MessageKey]"));
        assertThat((StreamMessage) argList.get(1), sameInstance(message));
    }
}
