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
package acromusashi.stream.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

/**
 * prepareメソッドに渡される設定を保持するBolt。
 * 
 * @author tsukano
 */
public abstract class BaseConfigurationBolt extends BaseRichBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 103046340453102132L;

    /** StormConfig */
    @SuppressWarnings("rawtypes")
    private Map               stormConf;

    /** TopologyContext */
    private TopologyContext   context;

    /** OutputCollector */
    private OutputCollector   collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector)
    {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    /**
     * Get StormConfig.
     * 
     * @return StormConfig
     */
    @SuppressWarnings("rawtypes")
    protected Map getStormConf()
    {
        return this.stormConf;
    }

    /**
     * Get TopologyContext.
     * 
     * @return TopologyContext
     */
    protected TopologyContext getContext()
    {
        return this.context;
    }

    /**
     * Get OutputCollector.
     * 
     * @return OutputCollector
     */
    protected OutputCollector getCollector()
    {
        return this.collector;
    }
}
