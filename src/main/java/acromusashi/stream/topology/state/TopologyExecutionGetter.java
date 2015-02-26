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
package acromusashi.stream.topology.state;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import acromusashi.stream.client.NimbusClientFactory;
import acromusashi.stream.exception.ConnectFailException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;

/**
 * Nimbusに問合せ、該当Topologyの実行状態を取得するクライアントクラス<br>
 * 該当Topologyのメッセージ処理数を定期的に確認し、指定した判定実行回数分変化がなかった場合に該当Topologyを「停止中」として扱う。<br>
 * メッセージ処理数に変化があった場合はメッセージ処理中として扱い、「処理中」として扱う。
 *
 * @author kimura
 */
public class TopologyExecutionGetter
{
    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public TopologyExecutionGetter()
    {
        // Do nothing.
    }

    /**
     * 該当Topologyの実行状態を取得する。
     *
     * @param topologyName トポロジ名
     * @param nimbusHost NimbusHost
     * @param nimbusPort NimbusPort
     * @param interval 処理中判定実行インターバル（ミリ秒単位）
     * @param number 処理中判定実行回数
     * @throws ConnectFailException 接続失敗時
     * @return Topology実行状態
     */
    public TopologyExecutionStatus getExecution(String topologyName, String nimbusHost,
            int nimbusPort, int interval, int number) throws ConnectFailException
    {

        NimbusClientFactory factory = new NimbusClientFactory();
        // RuntimeExceptionが発生
        NimbusClient nimbusClient = factory.createClient(nimbusHost, nimbusPort);
        Nimbus.Client client = nimbusClient.getClient();
        ClusterSummary clusterSummary = null;
        try
        {
            clusterSummary = client.getClusterInfo();
        }
        catch (org.apache.thrift7.TException ex)
        {
            throw new ConnectFailException(ex);
        }

        String topologyId = StormSummaryExtractor.extractStormTopologyId(clusterSummary,
                topologyName);

        if (topologyId == null)
        {
            return TopologyExecutionStatus.NOT_ALIVED;
        }

        TopologyExecutionCount compareCount = null;

        for (int getCount = 0; getCount < number; getCount++)
        {
            TopologyInfo topologyInfo = null;

            try
            {
                topologyInfo = client.getTopologyInfo(topologyId);
            }
            catch (NotAliveException ex)
            {
                return TopologyExecutionStatus.NOT_ALIVED;
            }
            catch (org.apache.thrift7.TException ex)
            {
                throw new ConnectFailException(ex);
            }

            TopologyExecutionCount nowCount = StormSummaryExtractor.convertToTotalCount(topologyInfo);
            // 初回の情報取得の場合、初回の取得結果を比較対象として設定
            if (compareCount == null)
            {
                compareCount = nowCount;
            }
            else
            {
                // 文字列表現が異なる場合、処理が継続しているため、ログ出力して「処理中」と返す。
                if (StringUtils.equals(compareCount.toString(), nowCount.toString()) == false)
                {
                    return TopologyExecutionStatus.EXECUTING;
                }
            }

            try
            {
                TimeUnit.MILLISECONDS.sleep(interval);
            }
            catch (InterruptedException ex)
            {
                continue;
            }
        }

        // 指定回数変化がなかった場合、停止として扱う。
        return TopologyExecutionStatus.STOP;
    }
}
