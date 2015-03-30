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
package acromusashi.stream.topology;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

/**
 * TrindentTopologyを実装する際の基底クラス
 * 
 * @author kimura
 */
public abstract class BaseTridentTopology
{
    /** logger */
    private static Logger logger = LoggerFactory.getLogger(BaseTridentTopology.class);

    /** TopologyName */
    protected String      topologyName;

    /** StormConfig */
    protected Config      config;

    /**
     * TopologyName、StormConfigを指定してTopologyを生成する。
     * @param topologyName TopologyName
     * @param config StormConfig
     */
    public BaseTridentTopology(String topologyName, Config config)
    {
        this.topologyName = topologyName;
        this.config = config;
    }

    /**
     * TridentTopologyを構築する。
     * 
     * @return 生成されたTridentTopology
     */
    public abstract StormTopology buildTridentTopology();

    /**
     * Topologyを実行する。
     * 
     * @param topology StormTopology
     * @param isLocal true:ローカルモード、false:分散モード
     * @throws Exception Topology実行失敗時
     */
    public void submitTopology(StormTopology topology, boolean isLocal) throws Exception
    {
        if (isLocal)
        {
            // ローカル環境で実行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(this.topologyName, this.config, topology);
        }
        else
        {
            // 分散環境で実行
            try
            {
                StormSubmitter.submitTopology(this.topologyName, this.config, topology);
            }
            catch (Exception ex)
            {
                String logFormat = "Occur exception at Topology Submit. Skip Topology Submit. : TopologyName={0}";
                logger.error(MessageFormat.format(logFormat, this.topologyName), ex);
            }
        }
    }
}
