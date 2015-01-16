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
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.trace.KeyHistory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * KeyTraceBaseBoltクラスのテストクラス<br>
 * モッククラスを用いて検証を行う。
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class AmBaseBoltTest
{
    /** テスト対象 */
    private KeyTraceBaseBolt target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector  mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map              mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext  mockContext;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new MockAmBaseBolt();
    }

    /**
     * Prepareメソッド呼び出し時の変数初期化確認を行う。
     *
     * @target {@link KeyTraceBaseBolt#prepare(Map, TopologyContext, OutputCollector)}
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
     * KeyHistoryInfo未保持Tuple受信時、空のKeyHistoryInfoを生成して動作を継続することを確認する。
     *
     * @target {@link KeyTraceBaseBolt#prepare(Map, TopologyContext, OutputCollector)}
     * @test 空のKeyHistoryInfoを生成して動作を継続することを確認
     *    condition:: KeyHistoryInfo未保持Tuple受信時
     *    result:: 空のKeyHistoryInfoを生成して動作を継続することを確認
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testExecute_KeyHistoryInfo未保持Tuple処理確認()
    {
        // 準備
        Mockito.when(this.mockContext.getThisComponentId()).thenReturn("ComponentId");
        Mockito.when(this.mockContext.getThisTaskIndex()).thenReturn(0);

        AmBaseThroughBolt targetBolt = new AmBaseThroughBolt();
        targetBolt.setFields(Arrays.asList("Key", "Message"));
        targetBolt.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        Tuple mockTuple = Mockito.mock(Tuple.class);
        Mockito.doThrow(new IllegalArgumentException()).when(mockTuple).getValueByField(
                FieldName.KEY_HISTORY);
        Mockito.doReturn("Key").when(mockTuple).getValueByField("Key");
        Mockito.doReturn("Message").when(mockTuple).getValueByField("Message");

        // 実施
        targetBolt.execute(mockTuple);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[]"));
        assertThat(argList.get(1).toString(), equalTo("Key"));
        assertThat(argList.get(2).toString(), equalTo("Message"));
    }

    /**
     * Tuple処理時、onExecuteメソッド実行中に例外が発生した場合、処理が終了することを確認する。
     *
     * @target {@link KeyTraceBaseBolt#prepare(Map, TopologyContext, OutputCollector)}
     * @test 処理が終了することを確認
     *    condition:: KeyHistoryInfo未保持Tuple受信時
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
        Mockito.doThrow(new RuntimeException()).when(mockedBolt).onExecute(any(Tuple.class));

        Tuple mockTuple = Mockito.mock(Tuple.class);
        Mockito.doReturn(new KeyHistory()).when(mockTuple).getValueByField(FieldName.KEY_HISTORY);
        Mockito.doReturn("Key").when(mockTuple).getValueByField("Key");
        Mockito.doReturn("Message").when(mockTuple).getValueByField("Message");

        // 実施
        mockedBolt.execute(mockTuple);

        // 検証
        fail("処理継続");
    }

    /**
     * DeclareOutputFieldsメソッド呼び出し時のフィールド確認を行う。
     *
     * @target {@link KeyTraceBaseBolt#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)}
     * @test 継承クラスの返すフィールドの頭に「KeyHistory」を付けて {@link OutputFieldsDeclarer#declare(Fields fields)}を呼び出していることを確認する。
     *    condition:: declareOutputFieldsメソッド実行
     *    result:: 継承クラスの返すフィールドの頭に「KeyHistory」を付けて {@link OutputFieldsDeclarer#declare(Fields fields)}を呼び出していること
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
        assertThat(argFields.size(), equalTo(3));
        assertThat(argFields.get(0), equalTo("keyHistory"));
        assertThat(argFields.get(1), equalTo("Key"));
        assertThat(argFields.get(2), equalTo("Message"));
    }

    /**
     * Anchor、Keyを指定していない状態でemitメソッド呼び出し確認を行う。
     *
     * @target {@link KeyTraceBaseBolt#emitWithNoAnchorKey(java.util.List)}
     * @test Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していることを確認する。
     *    condition:: KeyTraceBaseBolt#emitメソッド実行
     *    result:: Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_AnchorKey指定なし()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        KeyTraceBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();
        Tuple tuple = Mockito.mock(Tuple.class);
        KeyHistory history = new KeyHistory();
        Mockito.doReturn(history).when(tuple).getValueByField(anyString());
        testTarget.execute(tuple);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        testTarget.emitWithNoAnchorKey(objectList);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[]"));
        assertThat(argList.get(1), sameInstance(param1));
        assertThat(argList.get(2), sameInstance(param2));
    }

    /**
     * Keyを指定した状態でemitメソッド呼び出し確認を行う。
     *
     * @target {@link KeyTraceBaseBolt#emitWithNoAnchor(List, Object)}
     * @test Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していることを確認する。
     *    condition:: KeyTraceBaseBolt#emitメソッド実行
     *    result:: Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_MessageKey指定()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        KeyTraceBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();
        Tuple tuple = Mockito.mock(Tuple.class);
        KeyHistory history = new KeyHistory();
        Mockito.doReturn(history).when(tuple).getValueByField(anyString());
        testTarget.execute(tuple);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        testTarget.emitWithNoAnchor(objectList, "MessageKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
        assertThat(argList.get(1), sameInstance(param1));
        assertThat(argList.get(2), sameInstance(param2));
    }

    /**
     * Anchorを指定した状態でemitメソッド呼び出し確認を行う。
     *
     * @target {@link KeyTraceBaseBolt#emitWithNoKey(Tuple, List)}
     * @test Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していることを確認する。
     *    condition:: KeyTraceBaseBolt#emitメソッド実行
     *    result:: Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Anchor指定()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        KeyTraceBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();
        Tuple tuple = Mockito.mock(Tuple.class);
        Tuple anchor = Mockito.mock(Tuple.class);
        KeyHistory history = new KeyHistory();
        Mockito.doReturn(history).when(tuple).getValueByField(anyString());
        testTarget.execute(tuple);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        testTarget.emitWithNoKey(anchor, objectList);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Tuple> anchorArgument = ArgumentCaptor.forClass(Tuple.class);
        Mockito.verify(this.mockCollector).emit(anchorArgument.capture(), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[]"));
        assertThat(argList.get(1), sameInstance(param1));
        assertThat(argList.get(2), sameInstance(param2));

        assertThat(anchorArgument.getValue(), sameInstance(anchor));
    }

    /**
     * Anchor、MessageKeyを指定した状態でemitメソッド呼び出し確認を行う。
     *
     * @target {@link KeyTraceBaseBolt#emit(Tuple, List, Object)}
     * @test Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していることを確認する。
     *    condition:: KeyTraceBaseBolt#emitメソッド実行
     *    result:: Tuple中のKeyHistoryをTupleの頭に追加してCollectorを呼び出していること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_AnchorKey指定()
    {
        // 準備
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);

        KeyTraceBaseBolt testTarget = spy(this.target);
        Mockito.doNothing().when(testTarget).clearExecuteStatus();
        Tuple tuple = Mockito.mock(Tuple.class);
        Tuple anchor = Mockito.mock(Tuple.class);
        KeyHistory history = new KeyHistory();
        Mockito.doReturn(history).when(tuple).getValueByField(anyString());
        testTarget.execute(tuple);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        testTarget.emit(anchor, objectList, "MessageKey");

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Tuple> anchorArgument = ArgumentCaptor.forClass(Tuple.class);
        Mockito.verify(this.mockCollector).emit(anchorArgument.capture(), argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
        assertThat(argList.get(1), sameInstance(param1));
        assertThat(argList.get(2), sameInstance(param2));

        assertThat(anchorArgument.getValue(), sameInstance(anchor));
    }
}
