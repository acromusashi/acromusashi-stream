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
package acromusashi.stream.client;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;

import org.apache.thrift7.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * DrpcRequestClientのテストクラス
 * 
 * @author kimura
 */
public class DrpcRequestClientTest
{
    /** テスト対象 */
    private DrpcRequestClient target;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp() throws JoranException
    {
        this.target = new DrpcRequestClient();
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
     * 必須オプション未指定の状態で実行した場合、ヘルプが表示されてリクエストが送信されないことを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test ヘルプが表示されてリクエストが送信されないこと
     *    condition:: 必須オプション未指定の状態で実行した場合
     *    result:: ヘルプが表示されてリクエストが送信されないこと
     */
    @Test
    public void testStartSendRequest_必須オプション未指定()
    {
        // 準備
        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        Mockito.doNothing().when(spiedTarget).sendRequest(anyString(), anyInt(), anyInt(),
                anyString(), anyString());
        String[] args = {"-h", "DRPC1"};

        // 実施
        spiedTarget.startSendRequest(args);

        // 検証
        Mockito.verify(spiedTarget, times(0)).sendRequest(anyString(), anyInt(), anyInt(),
                anyString(), anyString());
    }

    /**
     * ヘルプオプション指定の状態で実行した場合、ヘルプが表示されてリクエストが送信されないことを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test ヘルプが表示されてリクエストが送信されないこと
     *    condition:: ヘルプオプション指定の状態で実行した場合
     *    result:: ヘルプが表示されてリクエストが送信されないこと
     */
    @Test
    public void testStartSendRequest_ヘルプオプション指定()
    {
        // 準備
        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        Mockito.doNothing().when(spiedTarget).sendRequest(anyString(), anyInt(), anyInt(),
                anyString(), anyString());
        String[] args = {"-h", "DRPC1", "-f", "function", "-a", "arg", "-sh"};

        // 実施
        spiedTarget.startSendRequest(args);

        // 検証
        Mockito.verify(spiedTarget, times(0)).sendRequest(anyString(), anyInt(), anyInt(),
                anyString(), anyString());
    }

    /**
     * 任意オプション未指定の状態で実行した場合、デフォルト値が使用されることを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test デフォルト値が使用されること
     *    condition:: 任意オプション未指定の状態で実行した場合
     *    result:: デフォルト値が使用されること
     */
    @Test
    public void testStartSendRequest_任意オプション未指定()
    {
        // 準備
        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        Mockito.doNothing().when(spiedTarget).sendRequest(anyString(), anyInt(), anyInt(),
                anyString(), anyString());
        String[] args = {"-h", "DRPC1", "-f", "function", "-a", "arg"};

        // 実施
        spiedTarget.startSendRequest(args);

        // 検証
        Mockito.verify(spiedTarget, times(1)).sendRequest("DRPC1", 3772, 30000, "function", "arg");
    }

    /**
     * 任意オプション指定の状態で実行した場合、指定した値が使用されることを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test 指定した値が使用されること
     *    condition:: 任意オプション指定の状態で実行した場合
     *    result:: 指定した値が使用されること
     */
    @Test
    public void testStartSendRequest_任意オプション指定()
    {
        // 準備
        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        Mockito.doNothing().when(spiedTarget).sendRequest(anyString(), anyInt(), anyInt(),
                anyString(), anyString());
        String[] args = {"-h", "DRPC1", "-p", "4772", "-t", "10000", "-f", "function", "-a", "arg"};

        // 実施
        spiedTarget.startSendRequest(args);

        // 検証
        Mockito.verify(spiedTarget, times(1)).sendRequest("DRPC1", 4772, 10000, "function", "arg");
    }

    /**
     * 接続失敗した場合に接続失敗した旨のログが出力されることを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test 接続失敗した旨のログが出力されること
     *    condition:: 接続失敗した場合
     *    result:: 接続失敗した旨のログが出力されること
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSendRequest_接続失敗()
    {
        // 準備
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        Mockito.when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);

        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        Mockito.doThrow(new RuntimeException()).when(spiedTarget).createClient(anyString(),
                anyInt(), anyInt());

        // 実施
        spiedTarget.sendRequest("DRPC1", 3772, 30000, "function", "arg");

        // 検証
        ArgumentCaptor<ILoggingEvent> eventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
        Mockito.verify(mockAppender).doAppend(eventCaptor.capture());

        ILoggingEvent actualEvent = eventCaptor.getValue();
        String actual = actualEvent.toString();

        assertThat(actual, equalTo("[ERROR] DRPCClient connect failed."));
    }

    /**
     * 送信失敗した場合に送信失敗した旨のログが出力されることを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test 接続失敗した旨のログが出力されること
     *    condition:: 接続失敗した場合
     *    result:: 接続失敗した旨のログが出力されること
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSendRequest_送信失敗() throws TException, DRPCExecutionException
    {
        // 準備
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        Mockito.when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);

        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        DRPCClient clientMock = Mockito.mock(DRPCClient.class);
        Mockito.doReturn(clientMock).when(spiedTarget).createClient(anyString(), anyInt(), anyInt());
        Mockito.doThrow(new TException()).when(clientMock).execute(anyString(), anyString());

        // 実施
        spiedTarget.sendRequest("DRPC1", 3772, 30000, "function", "arg");

        // 検証
        ArgumentCaptor<ILoggingEvent> eventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
        Mockito.verify(mockAppender).doAppend(eventCaptor.capture());

        ILoggingEvent actualEvent = eventCaptor.getValue();
        String actual = actualEvent.toString();

        assertThat(actual, equalTo("[ERROR] DRPCRequest failed."));
    }

    /**
     * 送信成功した場合に送信成功した旨のログが出力されることを確認する。
     * 
     * @target {@link DrpcRequestClient#startSendRequest(String...)}
     * @test 送信成功した旨のログが出力されること
     *    condition:: 送信成功した場合
     *    result:: 送信成功した旨のログが出力されること
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSendRequest_送信成功() throws TException, DRPCExecutionException
    {
        // 準備
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        Appender<ILoggingEvent> mockAppender = Mockito.mock(Appender.class);
        Mockito.when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);

        DrpcRequestClient spiedTarget = Mockito.spy(this.target);
        DRPCClient clientMock = Mockito.mock(DRPCClient.class);
        Mockito.doReturn(clientMock).when(spiedTarget).createClient(anyString(), anyInt(), anyInt());
        Mockito.doReturn("Succeed").when(clientMock).execute(anyString(), anyString());

        // 実施
        spiedTarget.sendRequest("DRPC1", 3772, 30000, "function", "arg");

        // 検証
        ArgumentCaptor<ILoggingEvent> eventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
        Mockito.verify(mockAppender).doAppend(eventCaptor.capture());

        ILoggingEvent actualEvent = eventCaptor.getValue();
        String actual = actualEvent.toString();

        assertThat(actual, equalTo("[INFO] DRPCRequest result is Succeed"));
    }
}
