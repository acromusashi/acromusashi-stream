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

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.utils.NimbusClient;

import com.google.common.collect.Maps;

/**
 * NimbusClient生成用のファクトリクラス
 *
 * @author kimura
 */
public class NimbusClientFactory
{
    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public NimbusClientFactory()
    {}

    /**
     * Nimbusへの接続クライアントを生成する。
     *
     * @param host Nimbusホスト
     * @param port NimbusPort
     * @return Nimbusへの接続クライアント
     */
    public NimbusClient createClient(String host, int port)
    {
        Map<String, Object> nimbusConf = Maps.newHashMap();
        nimbusConf.put(Config.NIMBUS_HOST, host);
        nimbusConf.put(Config.NIMBUS_THRIFT_PORT, port);
        nimbusConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
                "backtype.storm.security.auth.SimpleTransportPlugin");
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(nimbusConf);
        return nimbusClient;
    }
}
