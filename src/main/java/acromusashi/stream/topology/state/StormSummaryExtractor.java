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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

/**
 * StormNimbusから取得したStormのサマリ情報から情報を抽出するクラス
 *
 * @author kimura
 */
public class StormSummaryExtractor
{
    /** 全時間帯の処理件数を取得するキー */
    private static final String ALLTIME_KEY = ":all-time";

    /** DefaultStreamの処理件数を取得するキー */
    private static final String DEFAULT_KEY = "default";

    /**
     * 外部からのインスタンス化を防止するデフォルトコンストラクタ。
     */
    private StormSummaryExtractor()
    {
        // Do nothing.
    }

    /**
     * クラスタサマリ情報に対してトポロジ名を指定してトポロジIDを抽出する。
     *
     * @param clusterSummary クラスタサマリ情報
     * @param topologyName トポロジ名称
     * @return トポロジ名に対応するトポロジID。存在しない場合はnullを返す。
     */
    public static String extractStormTopologyId(ClusterSummary clusterSummary, String topologyName)
    {

        List<TopologySummary> topologyList = clusterSummary.get_topologies();

        for (TopologySummary targetTopology : topologyList)
        {
            // トポロジ名が一致しているトポロジ情報のIDを取得
            if (StringUtils.equals(topologyName, targetTopology.get_name()) == true)
            {
                return targetTopology.get_id();
            }
        }

        return null;
    }

    /**
     * トポロジ情報を基に処理カウントエンティティに変換する。
     *
     * @param topologyInfo トポロジ情報
     * @return 処理カウントエンティティ
     */
    public static TopologyExecutionCount convertToTotalCount(TopologyInfo topologyInfo)
    {
        List<ExecutorSummary> executors = topologyInfo.get_executors();
        TopologyExecutionCount totalCount = new TopologyExecutionCount();

        for (ExecutorSummary summary : executors)
        {
            ExecutorStats stats = summary.get_stats();

            Map<String, Map<String, Long>> emitStats = stats.get_emitted();
            long emitted = extractAllDefaultValue(emitStats);
            Map<String, Map<String, Long>> transferStats = stats.get_transferred();
            long transferred = extractAllDefaultValue(transferStats);

            long acked = 0;
            long failed = 0;

            ExecutorSpecificStats specificStats = stats.get_specific();

            if (specificStats.is_set_spout() == true)
            {
                SpoutStats spoutStats = specificStats.get_spout();
                Map<String, Map<String, Long>> ackStats = spoutStats.get_acked();
                acked = extractAllDefaultValue(ackStats);
                Map<String, Map<String, Long>> failStats = spoutStats.get_failed();
                failed = extractAllDefaultValue(failStats);
            }

            // 取得した値をエンティティに加算する。
            totalCount.setEmitted(totalCount.getEmitted() + emitted);
            totalCount.setTransferred(totalCount.getTransferred() + transferred);
            totalCount.setAcked(totalCount.getAcked() + acked);
            totalCount.setFailed(totalCount.getFailed() + failed);
        }

        return totalCount;
    }

    /**
     * 情報保持マップから「全時間帯」「デフォルトストリーム」処理件数を取得する。
     *
     * @param targetStats 情報保持マップ
     * @return 処理件数。存在しない値だった場合は0
     */
    public static long extractAllDefaultValue(Map<String, Map<String, Long>> targetStats)
    {

        Map<String, Long> allTimeMap = targetStats.get(ALLTIME_KEY);
        if (allTimeMap == null)
        {
            return 0L;
        }

        Long defaultValue = allTimeMap.get(DEFAULT_KEY);
        if (defaultValue == null)
        {
            return 0L;
        }

        return defaultValue;
    }
}
