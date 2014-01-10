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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift7.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * DrpcTridentEmitterのテストクラス
 * 
 * @author kimura
 */
public class DrpcTridentEmitterTest
{
    /** テスト対象 */
    private DrpcTridentEmitter target;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp() throws JoranException
    {
        this.target = new TestDrpcTridentEmitter();
        // パスを指定してLoggerを初期化
        LoggerContext lc = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure("src/test/resources/logback.xml");
    }

    /**
     * 各テストメソッド実行後の事後処理。
     */
    @After
    public void tearDown() throws JoranException
    {
        // パスを指定してLoggerを初期化
        LoggerContext lc = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure("src/test/resources/logback.xml");
    }

    /**
     * DrpcTridentEmitterの設定値初期化確認を行う。
     * 
     * @target {@link DrpcTridentEmitter#initialize(Map, TopologyContext, String)}
     * @test メンバ変数の初期化が行われること
     *    condition:: DrpcTridentEmitterの初期化を行う
     *    result:: メンバ変数の初期化が行われること
     */
    @SuppressWarnings("rawtypes")
    @Test
    public void testInitialize_設定値初期化確認()
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Map config = createBaseConfig();

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";

        // 実施
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        // 検証
        assertEquals(mockContext, spiedTarget.context);
        assertEquals(config, spiedTarget.stormConf);
        assertEquals("function", spiedTarget.function);
        assertEquals(0, spiedTarget.idsMap.size());
        assertEquals(30000, spiedTarget.rotateTime);
        assertEquals(11111L, spiedTarget.lastRotate);
    }

    /**
     * リクエストが存在しない状態でのDrpcTridentEmitterのemitBatch実行確認時、FetchHelperの初期化と取得が行われることを確認する。
     * 
     * @target {@link DrpcTridentEmitter#emitBatch(TransactionAttempt, Object, TridentCollector)}
     * @test FetchHelperの初期化と取得が行われること
     *    condition:: リクエストが存在しない状態でのDrpcTridentEmitterのemitBatch実行を行う
     *    result:: FetchHelperの初期化と取得が行われること
     */
    @Test
    public void testEmitBatch_リクエストなし() throws TException
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);

        // 検証
        assertTrue(spiedTarget.prepared);
        assertNotNull(spiedTarget.fetchHelper);
        Mockito.verify(mockHelper).fetch();
        Mockito.verify(spiedTarget, times(0)).emitTuples("function", mockCollector);
    }

    /**
     * Drpcリクエスト取得失敗時、取得失敗した旨のログが出力されることを確認する。
     * 
     * @target {@link DrpcTridentEmitter#emitBatch(TransactionAttempt, Object, TridentCollector)}
     * @test 取得失敗した旨のログが出力されること
     *    condition:: Drpcリクエスト取得失敗時
     *    result:: 取得失敗した旨のログが出力されること
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testEmitBatch_リクエスト取得失敗() throws TException
    {
        // 準備
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        Mockito.when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);

        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        Mockito.doThrow(new TException()).when(mockHelper).fetch();

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);

        // 検証
        assertTrue(spiedTarget.prepared);
        assertNotNull(spiedTarget.fetchHelper);
        Mockito.verify(mockHelper).fetch();
        Mockito.verify(spiedTarget, times(0)).emitTuples("function", mockCollector);

        ArgumentCaptor<ILoggingEvent> eventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
        Mockito.verify(mockAppender).doAppend(eventCaptor.capture());

        ILoggingEvent actualEvent = eventCaptor.getValue();
        String actual = actualEvent.toString();

        assertThat(actual, equalTo("[WARN] DRPC fetch failed."));
    }

    /**
     * リクエストが存在する状態でのDrpcTridentEmitterのemitBatch実行確認時、FetchHelperの初期化と取得が行われることを確認する。
     * 
     * @target {@link DrpcTridentEmitter#emitBatch(TransactionAttempt, Object, TridentCollector)}
     * @test FetchHelperの初期化と取得が行われること
     *    condition:: リクエストが存在する状態でのDrpcTridentEmitterのemitBatch実行を行う
     *    result:: FetchHelperの初期化と取得が行われること
     */
    @Test
    public void testEmitBatch_リクエストあり() throws TException
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.doReturn(requestInfo).when(mockHelper).fetch();

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);

        // 検証
        assertTrue(spiedTarget.prepared);
        assertNotNull(spiedTarget.fetchHelper);
        Mockito.verify(mockHelper).fetch();
        assertEquals(1, spiedTarget.idsMap.size());
        Mockito.verify(spiedTarget, times(1)).emitTuples("funcArg", mockCollector);
    }

    /**
     * 同一のトランザクションが複数回実行された場合に失敗として扱われることを確認する。（基本発生しないが、ガード措置）
     * 
     * @target {@link DrpcTridentEmitter#emitBatch(TransactionAttempt, Object, TridentCollector)}
     * @test 同一のトランザクションが複数回実行された場合に失敗として扱われること
     *    condition:: 同一のトランザクションが複数回実行された場合
     *    result:: 同一のトランザクションが複数回実行された場合に失敗として扱われること
     */
    @Test
    public void testEmitBatch_同一Transaction実行() throws TException
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.when(mockHelper.fetch()).thenReturn(requestInfo).thenReturn(null);

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);
        spiedTarget.emitBatch(attempt, meta, mockCollector);

        // 検証
        assertEquals(0, spiedTarget.idsMap.size());
        Mockito.verify(mockHelper).fail("11111");
    }

    /**
     * リクエストが受信後、タイムアウトした場合に失敗応答が返ることを確認する。
     * 
     * @target {@link DrpcTridentEmitter#emitBatch(TransactionAttempt, Object, TridentCollector)}
     * @test タイムアウトした場合に失敗応答が返ること
     *    condition:: リクエストが受信後、タイムアウト
     *    result:: タイムアウトした場合に失敗応答が返ること
     */
    @Test
    public void testEmitBatch_リクエスト受信後タイムアウト() throws TException
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.when(spiedTarget.getCurrentTime()).thenReturn(11111L).thenReturn(11111L).thenReturn(
                41112L);
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.when(mockHelper.fetch()).thenReturn(requestInfo).thenReturn(null);

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt1 = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        spiedTarget.emitBatch(attempt1, meta, mockCollector);

        // 時間経過を模倣
        spiedTarget.idsMap.rotate();
        spiedTarget.idsMap.rotate();

        // 実施
        TransactionAttempt attempt2 = new TransactionAttempt(22222L, 222);
        spiedTarget.emitBatch(attempt2, meta, mockCollector);

        // 検証
        assertEquals(0, spiedTarget.idsMap.size());
        Mockito.verify(mockHelper).fail("11111");
    }

    /**
     * リクエストが受信後、タイムアウトした場合に失敗応答を返し、失敗応答を返せなかった場合のログ出力されることを確認する
     * 
     * @target {@link DrpcTridentEmitter#emitBatch(TransactionAttempt, Object, TridentCollector)}
     * @test 応答失敗したことがログ出力されること
     *    condition:: リクエストが受信後、タイムアウトした場合に失敗応答を返し、失敗応答を返せなかった場合
     *    result:: 応答失敗したことがログ出力されること
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testEmitBatch_リクエスト受信後タイムアウト_応答失敗() throws TException
    {
        // 準備
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        Mockito.when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);

        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.when(spiedTarget.getCurrentTime()).thenReturn(11111L).thenReturn(11111L).thenReturn(
                41112L);
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.when(mockHelper.fetch()).thenReturn(requestInfo).thenReturn(null);
        Mockito.doThrow(new TException()).when(mockHelper).fail("11111");

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt1 = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        spiedTarget.emitBatch(attempt1, meta, mockCollector);

        // 時間経過を模倣
        spiedTarget.idsMap.rotate();
        spiedTarget.idsMap.rotate();

        // 実施
        TransactionAttempt attempt2 = new TransactionAttempt(22222L, 222);
        spiedTarget.emitBatch(attempt2, meta, mockCollector);

        // 検証
        assertTrue(spiedTarget.prepared);
        assertNotNull(spiedTarget.fetchHelper);
        assertEquals(0, spiedTarget.idsMap.size());
        Mockito.verify(mockHelper).fail("11111");

        ArgumentCaptor<ILoggingEvent> eventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
        Mockito.verify(mockAppender).doAppend(eventCaptor.capture());

        ILoggingEvent actualEvent = eventCaptor.getValue();
        String actual = actualEvent.toString();

        assertThat(
                actual,
                equalTo("[WARN] Fail notify failed. TransactionAttempt=11111:111, DrpcRequestInfo=DrpcRequestInfo[requestId=11111,funcArgs=funcArg]"));
    }

    /**
     * 未受信リクエストに対して応答が返った場合、DRPCClientに応答が返らないことを確認する。
     * 
     * @target {@link DrpcTridentEmitter#success(TransactionAttempt)}
     * @test DRPCClientに応答が返らないこと
     *    condition:: 未受信リクエストに対して応答が返った場合
     *    result:: DRPCClientに応答が返らないこと
     */
    @Test
    public void testSuccess_リクエスト未受信時未応答() throws TException
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.doReturn(requestInfo).when(mockHelper).fetch();

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);
        spiedTarget.idsMap.remove(attempt);
        spiedTarget.success(attempt);

        // 検証
        Mockito.verify(mockHelper, times(0)).ack("11111", "Succeeded");
        assertEquals(0, spiedTarget.idsMap.size());
    }

    /**
     * リクエストに対して応答が返った場合、DRPCClientに応答が返ることを確認する。
     * 
     * @target {@link DrpcTridentEmitter#success(TransactionAttempt)}
     * @test DRPCClientに応答が返ること
     *    condition:: リクエストに対して応答が返った場合
     *    result:: DRPCClientに応答が返ること
     */
    @Test
    public void testSuccess_リクエスト完了() throws TException
    {
        // 準備
        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.doReturn(requestInfo).when(mockHelper).fetch();

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);
        spiedTarget.success(attempt);

        // 検証
        Mockito.verify(mockHelper).ack("11111", "Succeeded");
        assertEquals(0, spiedTarget.idsMap.size());
    }

    /**
     * リクエストに対して応答が返り、DRPCClientに対して応答を返せなかった場合、結果がログ出力されることを確認する。
     * 
     * @target {@link DrpcTridentEmitter#success(TransactionAttempt)}
     * @test 結果がログ出力されること
     *    condition:: リクエストに対して応答が返り、DRPCClientに対して応答を返せなかった場合
     *    result:: 結果がログ出力されること
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSuccess_リクエスト完了_応答失敗() throws TException
    {
        // 準備
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        Mockito.when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);

        DrpcTridentEmitter spiedTarget = Mockito.spy(this.target);
        DrpcFetchHelper mockHelper = Mockito.mock(DrpcFetchHelper.class);
        Mockito.doReturn(11111L).when(spiedTarget).getCurrentTime();
        Mockito.doReturn(mockHelper).when(spiedTarget).createFetchHelper();
        DrpcRequestInfo requestInfo = new DrpcRequestInfo();
        requestInfo.setRequestId("11111");
        requestInfo.setFuncArgs("funcArg");
        Mockito.doReturn(requestInfo).when(mockHelper).fetch();
        Mockito.doThrow(new TException()).when(mockHelper).ack("11111", "Succeeded");

        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        String function = "function";
        spiedTarget.initialize(createBaseConfig(), mockContext, function);

        TransactionAttempt attempt = new TransactionAttempt(11111L, 111);
        TridentCollector mockCollector = Mockito.mock(TridentCollector.class);
        Object meta = new Object();

        // 実施
        spiedTarget.emitBatch(attempt, meta, mockCollector);
        spiedTarget.success(attempt);

        // 検証
        Mockito.verify(mockHelper).ack("11111", "Succeeded");
        assertEquals(0, spiedTarget.idsMap.size());

        ArgumentCaptor<ILoggingEvent> eventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
        Mockito.verify(mockAppender).doAppend(eventCaptor.capture());

        ILoggingEvent actualEvent = eventCaptor.getValue();
        String actual = actualEvent.toString();

        assertThat(
                actual,
                equalTo("[WARN] Success notify failed. TransactionAttempt=11111:111, DrpcRequestInfo=DrpcRequestInfo[requestId=11111,funcArgs=funcArg]"));
    }

    /**
     * 基本の設定オブジェクトを生成する。
     * 
     * @return 基本の設定オブジェクト
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map createBaseConfig()
    {
        Map conf = new HashMap();

        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);
        conf.put(Config.DRPC_SERVERS, Arrays.asList("DRPC1"));
        conf.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        return conf;
    }
}
