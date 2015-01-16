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
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.trace.KeyHistory;
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
public class KeyTraceBaseSpoutTest
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
        this.target = new MockKeyTraceBaseSpout();
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
     * @test 継承クラスの返すフィールドの頭に「KeyHistory」を付けて {@link OutputFieldsDeclarer#declare(Fields fields)}を呼び出していることを確認する。<br>
     *    condition:: declareOutputFieldsメソッド実行<br>
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
     * Emitメソッド呼び出し時（KeyId未指定）の引数確認を行う。
     *
     * @target {@link KeyTraceBaseSpout#emit(List<Object>)}
     * @test collectorにemitされるTupleの頭にはkeyHistoryが設定されていること<br>
     *    condition:: MessageKey、MessageIdを指定せずにTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleの頭にはkeyHistoryが設定されていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId未指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        this.target.emitWithNoKeyId(null);

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
     * Emitメソッド呼び出し時（Key指定）の引数確認を行う。
     *
     * @target {@link KeyTraceBaseSpout#emit(List<Object>, Object)}
     * @test collectorにemitされるTupleの頭にはKeyを保持するkeyHistoryが設定されていること<br>
     *    condition:: MessageKeyを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleの頭にはKeyを保持するkeyHistoryが設定されていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_Key指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        this.target.emitWithKey(objectList, "MessageKey");

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
     * Emitメソッド呼び出し時（KeyId指定）の引数確認を行う。
     *
     * @target {@link KeyTraceBaseSpout#emit(List<Object>, Object, Object)}
     * @test collectorにemitされるTupleの頭にはKeyを保持するkeyHistoryが設定されていること<br>
     *    condition:: MessageKey、MessageIdを指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleの頭にはKeyを保持するkeyHistoryが設定されていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        this.target.emit(null, "MessageKeyId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageKeyId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[MessageKeyId]"));
        assertThat(argList.get(1), sameInstance(param1));
        assertThat(argList.get(2), sameInstance(param2));
    }

    /**
     * Emitメソッド呼び出し時（KeyId個別指定）の引数確認を行う。
     *
     * @target {@link KeyTraceBaseSpout#emit(List<Object>, Object, Object)}
     * @test collectorにemitされるTupleの頭にはKeyを保持するkeyHistoryが設定されていること<br>
     *    condition:: MessageKey、MessageIdを個別指定してTupleのemitを行う。<br>
     *    result:: collectorにemitされるTupleの頭にはKeyを保持するkeyHistoryが設定されていること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testEmit_KeyId個別指定()
    {
        // 準備
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);

        List<Object> objectList = new ArrayList<Object>();
        Object param1 = new Object();
        Object param2 = new Object();
        objectList.add(param1);
        objectList.add(param2);

        // 実施
        this.target.emitWithKeyId(null, "MessageKey", "MessageId");

        // 検証
        ArgumentCaptor<List> tupleArgument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(tupleArgument.capture(), eq("MessageId"));

        List<Object> argList = tupleArgument.getValue();

        assertThat(argList.size(), equalTo(3));
        assertThat(argList.get(0), instanceOf(KeyHistory.class));
        assertThat(argList.get(0).toString(), equalTo("KeyHistory=[MessageKey]"));
        assertThat(argList.get(1), sameInstance(param1));
        assertThat(argList.get(2), sameInstance(param2));
    }
}
