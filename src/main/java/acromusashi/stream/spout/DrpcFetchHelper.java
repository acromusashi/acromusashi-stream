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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.utils.Utils;

/**
 * DRPCリクエストをDRPCServerから受信し、Serverに即応答を返すヘルパークラス。<br>
 * DRPCリクエストを受けて非同期で処理を起動／実行する場合に使用する。<br>
 * 
 * @author kimura
 */
public class DrpcFetchHelper
{
    /** DRPCリクエストを取得するクライアントリスト */
    protected List<DRPCInvocationsClient> clients = new ArrayList<DRPCInvocationsClient>();

    /** DRPCで受信する機能名 */
    protected String                      function;

    /** DRPCリクエスト情報を保持するマップ */
    protected Map<String, Integer>        requestedMap;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public DrpcFetchHelper()
    {}

    /**
     * 初期化処理を行う。
     * 
     * @param conf Stormの設定オブジェクト
     * @param func DRPC取得機能
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void initialize(Map conf, String func)
    {
        List<String> servers = (List<String>) conf.get(Config.DRPC_SERVERS);
        int port = Utils.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT));
        this.function = func;
        this.requestedMap = new HashMap<String, Integer>();

        // DRPCClientを設定値から生成
        for (String server : servers)
        {
            this.clients.add(createInvocationClient(server, port));
        }
    }

    /**
     * DRPCリクエストが存在していないか取得確認を行う。<br>
     * 登録されているサーバリストから順に確認を行い、リクエストが取得できた時点で正常応答をDRPCServer側に返信し、<br>
     * 機能呼び出し時にリクエスト情報を呼び出し元に返す。
     * 
     * @return DRPCリクエストが存在している場合、リクエスト情報、リクエストが存在しない場合はnull
     * @throws TException 取得失敗時
     */
    public DrpcRequestInfo fetch() throws TException
    {
        DrpcRequestInfo result = null;

        for (int index = 0; index < this.clients.size(); index++)
        {
            DRPCInvocationsClient client = this.clients.get(index);
            DRPCRequest request = client.fetchRequest(this.function);
            String requestId = request.get_request_id();
            if (requestId.length() > 0)
            {
                result = new DrpcRequestInfo();
                result.setRequestId(requestId);
                result.setFuncArgs(request.get_func_args());
                this.requestedMap.put(requestId, index);
                break;
            }
        }

        return result;
    }

    /**
     * 処理が成功した場合の応答を返す。
     * 
     * @param requestId リクエストID
     * @param resultMessage 応答メッセージ
     * @throws TException 応答を返す際に失敗した場合
     */
    public void ack(String requestId, String resultMessage) throws TException
    {
        int index = this.requestedMap.remove(requestId);
        DRPCInvocationsClient client = this.clients.get(index);
        client.result(requestId, resultMessage);
    }

    /**
     * 処理が失敗した場合の応答を返す。
     * 
     * @param requestId リクエストID
     * @throws TException 応答を返す際に失敗した場合
     */
    public void fail(String requestId) throws TException
    {
        int index = this.requestedMap.remove(requestId);
        DRPCInvocationsClient client = this.clients.get(index);
        client.failRequest(requestId);
    }

    /**
     * DRPCリクエストを取得するクライアントを生成する。
     * 
     * @param host DRPCServerホスト
     * @param port DRPCServerInvocation用ポート
     * @return 生成クライアント
     */
    protected DRPCInvocationsClient createInvocationClient(String host, int port)
    {
        return new DRPCInvocationsClient(host, port);
    }
}
