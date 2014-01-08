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
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Topologyを実装する際の基底クラス
 * 
 * @author tsukano
 */
public abstract class BaseTopology
{
    /** logger */
    private static final Logger logger  = LoggerFactory.getLogger(BaseTopology.class);

    /** TopologyBuilder */
    private TopologyBuilder     builder = new TopologyBuilder();

    /** TopologyName */
    private String              topologyName;

    /** StormConfig */
    private Config              config;

    /**
     * TopologyName、StormConfigを指定してTopologyを生成する。
     * @param topologyName TopologyName
     * @param config StormConfig
     */
    public BaseTopology(String topologyName, Config config)
    {
        this.topologyName = topologyName;
        this.config = config;
    }

    /**
     * @return the builder
     */
    protected TopologyBuilder getBuilder()
    {
        return this.builder;
    }

    /**
     * @return the topologyName
     */
    protected String getTopologyName()
    {
        return this.topologyName;
    }

    /**
     * @return the config
     */
    protected Config getConfig()
    {
        return this.config;
    }

    /**
     * Topologyを構築する。
     * @throws Exception Topology構築時に発生した例外
     */
    public abstract void buildTopology() throws Exception;

    /**
     * Topologyを実行する。
     * @param isLocal true:ローカルモード、false:分散モード
     */
    public void submitTopology(boolean isLocal)
    {
        if (logger.isInfoEnabled())
        {
            logger.info("isLocal=" + isLocal);
        }

        if (isLocal)
        {
            // ローカル環境で実行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(getTopologyName(), getConfig(),
                    getBuilder().createTopology());
        }
        else
        {
            // 分散環境で実行
            try
            {
                StormSubmitter.submitTopology(getTopologyName(), getConfig(),
                        getBuilder().createTopology());
            }
            catch (AlreadyAliveException ex)
            {
                String logFormat = "Occur exception at Topology Submit. Skip Topology Submit. : TopologyName={0}";
                String logMessage = MessageFormat.format(logFormat,
                        getTopologyName());
                logger.error(logMessage, ex);
            }
            catch (InvalidTopologyException ex)
            {
                String logFormat = "Occur exception at Topology Submit. Skip Topology Submit. : TopologyName={0}";
                String logMessage = MessageFormat.format(logFormat,
                        getTopologyName());
                logger.error(logMessage, ex);
            }
        }
    }

}
