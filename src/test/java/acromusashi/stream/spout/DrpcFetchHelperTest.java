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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import backtype.storm.Config;
import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.generated.DRPCRequest;

/**
 * DrpcFetchHelperのテストクラス
 * 
 * @author kimura
 */
public class DrpcFetchHelperTest
{
    /** テスト対象 */
    private DrpcFetchHelper target;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new DrpcFetchHelper();
    }

    /**
     * DRPCServer設定値が1個の状態でDRPCInvocationClientが1個生成されることを確認する。
     * 
     * @target {@link DrpcFetchHelper#initialize(java.util.Map, String)}
     * @test DRPCInvocationClientが1個生成されること
     *    condition:: DRPCServer設定値が1個の状態でDrpcFetchHelperを初期化
     *    result:: DRPCInvocationClientが1個生成されること
     */
    @Test
    public void testInitialize_DRPCServer設定1個()
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.doReturn(clientMock).when(spiedTarget).createInvocationClient(anyString(), anyInt());

        // 実施
        spiedTarget.initialize(config, "function");

        // 検証
        assertEquals(1, spiedTarget.clients.size());
    }

    /**
     * DRPCServer設定値が2個の状態でDRPCInvocationClientが2個生成されることを確認する。
     * 
     * @target {@link DrpcFetchHelper#initialize(java.util.Map, String)}
     * @test DRPCInvocationClientが2個生成されること
     *    condition:: DRPCServer設定値が2個の状態でDrpcFetchHelperを初期化
     *    result:: DRPCInvocationClientが2個生成されること
     */
    @Test
    public void testInitialize_DRPCServer設定2個()
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        servers.add("DRPC2");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock = Mockito.mock(DRPCInvocationsClient.class);
        Mockito.doReturn(clientMock).when(spiedTarget).createInvocationClient(anyString(), anyInt());

        // 実施
        spiedTarget.initialize(config, "function");

        // 検証
        assertEquals(2, spiedTarget.clients.size());
    }

    /**
     * DRPCServerリクエストが存在しない状態でリクエストが受信されないことを確認する。
     * 
     * @target {@link DrpcFetchHelper#fetch()}
     * @test リクエストが受信されないこと
     *    condition:: DRPCServerリクエストが存在しない状態でfetchメソッドを実行
     *    result:: リクエストが受信されないこと
     */
    @Test
    public void testFetch_リクエスト未存在() throws TException
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock = Mockito.mock(DRPCInvocationsClient.class);
        DRPCRequest blankRequest = new DRPCRequest("", "");
        Mockito.doReturn(blankRequest).when(clientMock).fetchRequest(anyString());
        Mockito.doReturn(clientMock).when(spiedTarget).createInvocationClient(anyString(), anyInt());

        // Helperの初期化
        spiedTarget.initialize(config, "function");

        // 実施
        DrpcRequestInfo actual = spiedTarget.fetch();

        // 検証
        Mockito.verify(clientMock).fetchRequest("function");
        assertNull(actual);
    }

    /**
     * DRPCServerリクエストが2クライアント目に存在する状態でリクエストが受信されることを確認する。
     * 
     * @target {@link DrpcFetchHelper#fetch()}
     * @test リクエストが受信されること
     *    condition:: DRPCServerリクエストが2クライアント目に存在する状態でfetchメソッドを実行
     *    result:: リクエストが受信されること
     */
    @Test
    public void testFetch_リクエスト存在_2クライアント目() throws TException
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        servers.add("DRPC2");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock1 = Mockito.mock(DRPCInvocationsClient.class);
        DRPCInvocationsClient clientMock2 = Mockito.mock(DRPCInvocationsClient.class);
        DRPCRequest blankRequest = new DRPCRequest("", "");
        DRPCRequest drpcRequest = new DRPCRequest("args", "11111");
        Mockito.doReturn(blankRequest).when(clientMock1).fetchRequest(anyString());
        Mockito.doReturn(drpcRequest).when(clientMock2).fetchRequest(anyString());
        Mockito.doReturn(clientMock1).when(spiedTarget).createInvocationClient("DRPC1", 3773);
        Mockito.doReturn(clientMock2).when(spiedTarget).createInvocationClient("DRPC2", 3773);

        // Helperの初期化
        spiedTarget.initialize(config, "function");

        // 実施
        DrpcRequestInfo actual = spiedTarget.fetch();

        // 検証
        Mockito.verify(clientMock1).fetchRequest("function");
        Mockito.verify(clientMock2).fetchRequest("function");
        assertEquals("11111", actual.getRequestId());
        assertEquals("args", actual.getFuncArgs());
        assertEquals(1, spiedTarget.requestedMap.size());
        assertTrue(spiedTarget.requestedMap.containsKey("11111"));
        assertEquals(1, spiedTarget.requestedMap.get("11111").intValue());
    }

    /**
     * DRPCServerリクエストが1クライアント目に存在する状態でリクエストが受信されることを確認する。
     * 
     * @target {@link DrpcFetchHelper#fetch()}
     * @test リクエストが受信されること
     *    condition:: DRPCServerリクエストが1クライアント目に存在する状態でfetchメソッドを実行
     *    result:: リクエストが受信されること
     */
    @Test
    public void testFetch_リクエスト存在_1クライアント目() throws TException
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock = Mockito.mock(DRPCInvocationsClient.class);
        DRPCRequest drpcRequest = new DRPCRequest("args", "11111");
        Mockito.doReturn(drpcRequest).when(clientMock).fetchRequest(anyString());
        Mockito.doReturn(clientMock).when(spiedTarget).createInvocationClient("DRPC1", 3773);

        // Helperの初期化
        spiedTarget.initialize(config, "function");

        // 実施
        DrpcRequestInfo actual = spiedTarget.fetch();

        // 検証
        Mockito.verify(clientMock).fetchRequest("function");
        assertEquals("11111", actual.getRequestId());
        assertEquals("args", actual.getFuncArgs());
        assertEquals(1, spiedTarget.requestedMap.size());
        assertTrue(spiedTarget.requestedMap.containsKey("11111"));
        assertEquals(0, spiedTarget.requestedMap.get("11111").intValue());
    }

    /**
     * DRPCServerリクエストに対してAckを実行した場合にClientに対してAckが通知されることを確認する。
     * 
     * @target {@link DrpcFetchHelper#ack(String, String)}
     * @test Clientに対してAckが通知されること
     *    condition:: DRPCServerリクエスト受信後、Ackメソッドを実行
     *    result:: Clientに対してAckが通知されること
     */
    @Test
    public void testAck_Ack返信() throws TException
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock = Mockito.mock(DRPCInvocationsClient.class);
        DRPCRequest drpcRequest = new DRPCRequest("args", "11111");
        Mockito.doReturn(drpcRequest).when(clientMock).fetchRequest(anyString());
        Mockito.doReturn(clientMock).when(spiedTarget).createInvocationClient("DRPC1", 3773);

        // Helperの初期化
        spiedTarget.initialize(config, "function");
        spiedTarget.fetch();

        // 実施
        spiedTarget.ack("11111", "Result Message");

        // 検証
        Mockito.verify(clientMock).result("11111", "Result Message");
        assertEquals(0, spiedTarget.requestedMap.size());
    }

    /**
     * DRPCServerリクエストに対してFailを実行した場合にClientに対してFailが通知されることを確認する。
     * 
     * @target {@link DrpcFetchHelper#fail(String)}
     * @test Clientに対してFailが通知されること
     *    condition:: DRPCServerリクエスト受信後、Failメソッドを実行
     *    result:: Clientに対してFailが通知されること
     */
    @Test
    public void testFail_Fail返信() throws TException
    {
        // 準備
        Map<String, Object> config = new HashMap<String, Object>();
        List<String> servers = new ArrayList<String>();
        servers.add("DRPC1");
        config.put(Config.DRPC_SERVERS, servers);
        config.put(Config.DRPC_INVOCATIONS_PORT, 3773);

        DrpcFetchHelper spiedTarget = Mockito.spy(this.target);
        DRPCInvocationsClient clientMock = Mockito.mock(DRPCInvocationsClient.class);
        DRPCRequest drpcRequest = new DRPCRequest("args", "11111");
        Mockito.doReturn(drpcRequest).when(clientMock).fetchRequest(anyString());
        Mockito.doReturn(clientMock).when(spiedTarget).createInvocationClient("DRPC1", 3773);

        // Helperの初期化
        spiedTarget.initialize(config, "function");
        spiedTarget.fetch();

        // 実施
        spiedTarget.fail("11111");

        // 検証
        Mockito.verify(clientMock).failRequest("11111");
        assertEquals(0, spiedTarget.requestedMap.size());
    }
}
