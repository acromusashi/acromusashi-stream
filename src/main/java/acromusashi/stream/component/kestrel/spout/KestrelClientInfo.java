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
package acromusashi.stream.component.kestrel.spout;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.KestrelThriftClient;

public class KestrelClientInfo
{
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger(KestrelClientInfo.class);

    /** 無効化リミット時刻 */
    public Long                 blacklistTillTimeMs;

    /** ホスト */
    public String               host;

    /** ポート */
    public int                  port;

    /** Kestrel接続クライアントオブジェクト */
    private KestrelThriftClient client;

    /**
     * ホスト名、ポートを指定してインスタンスを生成する。
     * 
     * @param host ホスト名
     * @param port ポート
     */
    public KestrelClientInfo(String host, int port)
    {
        this.host = host;
        this.port = port;
        this.blacklistTillTimeMs = 0L;
        this.client = null;
    }

    /**
     * Kestrelへの接続クライアントを取得する。
     * 
     * @return Kestrelへの接続クライアント
     * @throws TException 接続失敗時
     */
    public KestrelThriftClient getValidClient() throws TException
    {
        if (this.client == null)
        { // If client was blacklisted, remake it.
            logger.info("Attempting reconnect to kestrel " + this.host + ":" + this.port);
            this.client = new KestrelThriftClient(this.host, this.port);
        }

        return this.client;
    }

    /**
     * Kestrelへの接続クライアントを破棄する。
     */
    public void closeClient()
    {
        if (this.client != null)
        {
            this.client.close();
            this.client = null;
        }
    }

    /**
     * @return the client
     */
    public KestrelThriftClient getClient()
    {
        return this.client;
    }
}
