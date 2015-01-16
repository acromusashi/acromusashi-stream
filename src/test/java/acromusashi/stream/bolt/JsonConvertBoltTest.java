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
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessageHeader;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * JacksonMessageConvertBoltのテストケース
 * 
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonConvertBoltTest
{
    /** 試験用データファイル配置ディレクトリ*/
    private static final String                       DATA_DIR = "src/test/resources/"
                                                                       + StringUtils.replaceChars(
                                                                               JsonConvertBoltTest.class.getPackage().getName(),
                                                                               '.', '/') + '/';

    /** テスト対象 */
    private JsonConvertBolt<TestUserEntity> target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector                           mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map                                       mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext                           mockContext;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new JsonConvertBolt<TestUserEntity>();
        this.target.setUserObjectClass(TestUserEntity.class);
        BaseConfigurationBolt baseBolt = (BaseConfigurationBolt) this.target;
        baseBolt.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
    }

    /**
     * Prepareメソッド実行後、各コンポーネントの初期化が行われることの確認を行う
     * 
     * @target {@link JsonConvertBolt#prepare(Map, TopologyContext, OutputCollector)}
     * @test 各コンポーネントの初期化が行われること
     *    condition:: Prepareメソッド実行
     *    result:: 各コンポーネントの初期化が行われること
     */
    @Test
    public void testOnPrepare_コンポーネント初期化確認()
    {
        // 検証
        assertNotNull(this.target.objectMapper);
    }

    /**
     * UserObjectをエンティティ変換した際、次のBoltにemitされることの確認を行う。 
     * 
     * @target {@link JsonConvertBolt#execute(backtype.storm.tuple.Tuple)}
     * @test 次のBoltにemitされること
     *    condition::  UserObjectメッセージを含むTupleを受信
     *    result:: 次のBoltにemitされること
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testExecute_UserObjectメッセージ変換() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        String messageKey = "192.168.100.31";
        String messageStr = FileUtils.readFileToString(new File(DATA_DIR
                + "JacksonMessageConvertBoltTest_Valid.txt"));
        Mockito.doReturn(messageKey).when(mockTuple).getStringByField(FieldName.MESSAGE_KEY);
        Mockito.doReturn(messageStr).when(mockTuple).getStringByField(FieldName.MESSAGE_VALUE);

        StreamMessage expected = baseExpectedMessage();

        // 実施
        this.target.execute(mockTuple);

        // 検証
        ArgumentCaptor<Tuple> anchorArgument = ArgumentCaptor.forClass(Tuple.class);
        ArgumentCaptor<List> emitTupleArgument = ArgumentCaptor.forClass(List.class);

        Mockito.verify(mockCollector).emit(anchorArgument.capture(), emitTupleArgument.capture());

        Tuple anchor = anchorArgument.getValue();
        assertThat(anchor, sameInstance(mockTuple));

        List<Object> argList = emitTupleArgument.getValue();

        assertThat(argList.size(), equalTo(2));
        assertThat(argList.get(0).toString(), equalTo("192.168.100.31"));
        assertThat(argList.get(1).toString(), equalTo(expected.toString()));
    }

    /**
     * 非JSONメッセージをエンティティ変換した際、次のBoltにemitが行なわれないことの確認を行う。
     * 
     * @target {@link JsonConvertBolt#execute(backtype.storm.tuple.Tuple)}
     * @test 次Boltにemitが行われないこと
     *    condition:: 非JSONメッセージを含むTupleを受信
     *    result:: 次Boltにemitが行われないこと
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testExecute_非JSONメッセージ変換() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        String messageKey = "126.0.0.31";
        String messageStr = FileUtils.readFileToString(new File(DATA_DIR
                + "JacksonMessageConvertBoltTest_NotJson.txt"));
        Mockito.doReturn(messageKey).when(mockTuple).getStringByField(FieldName.MESSAGE_KEY);
        Mockito.doReturn(messageStr).when(mockTuple).getStringByField(FieldName.MESSAGE_VALUE);

        // 実施
        this.target.execute(mockTuple);

        // 検証
        Mockito.verify(this.mockCollector, times(0)).emit(any(Tuple.class), anyList());
    }

    /**
     * UserObject不正形式のメッセージをエンティティ変換した際、次のBoltにemitが行なわれないことの確認を行う。
     * 
     * @target {@link JsonConvertBolt#execute(backtype.storm.tuple.Tuple)}
     * @test 次Boltにemitが行われないこと
     *    condition:: UserObject不正形式のメッセージを含むTupleを受信
     *    result:: 次Boltにemitが行われないこと
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testOnExecute_UserObject不正形式メッセージ変換() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        String messageKey = "126.0.0.31";
        String messageStr = FileUtils.readFileToString(new File(DATA_DIR
                + "JacksonMessageConvertBoltTest_InValidUserObject.txt"));
        Mockito.doReturn(messageKey).when(mockTuple).getStringByField(FieldName.MESSAGE_KEY);
        Mockito.doReturn(messageStr).when(mockTuple).getStringByField(FieldName.MESSAGE_VALUE);

        // 実施
        this.target.execute(mockTuple);

        // 検証
        Mockito.verify(this.mockCollector, times(0)).emit(any(Tuple.class), anyList());
    }

    /**
     * 検証用の共通メッセージエンティティを作成する
     * 
     * @return 共通メッセージエンティティ
     */
    private StreamMessage baseExpectedMessage()
    {
        StreamMessage message = new StreamMessage();
        StreamMessageHeader header = new StreamMessageHeader();
        header.setMessageId("192.168.100.31_20130419182101127_0019182101");
        header.setMessageKey("192.168.100.31");
        header.setSource("192.168.100.31");
        header.setType("snmp");
        header.setVersion("v2c");
        message.setHeader(header);
        message.setBody(createUserEntity());

        return message;
    }

    /**
     * 検証用のユーザエンティティを作成する
     * 
     * @return 検証用ユーザエンティティ
     */
    private TestUserEntity createUserEntity()
    {
        TestUserEntity entity = new TestUserEntity();

        entity.setMessageId("192.168.100.99_20130419182101127_0019182101");
        entity.setMessageKey("192.168.100.99");

        return entity;
    }
}
