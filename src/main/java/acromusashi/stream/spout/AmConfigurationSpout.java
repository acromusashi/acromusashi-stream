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

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;

/**
 * BaseSpout, this class has the following values ​​in the field.
 *
 * <ol>
 * <li>Storm configuration</li>
 * <li>Topology context</li>
 * <li>SpoutOutputCollector</li>
 * </ol>
 *
 * @author kimura
 */
public abstract class AmConfigurationSpout extends BaseRichSpout
{
    /** serialVersionUID */
    private static final long    serialVersionUID = 6538721209916998171L;

    /** StormConfig */
    @SuppressWarnings("rawtypes")
    private Map                  stormConf;

    /** TopologyContext */
    private TopologyContext      context;

    /** SpoutOutputCollector */
    private SpoutOutputCollector collector;

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    /**
     * Get Storm configuration.
     *
     * @return Storm configuration
     */
    @SuppressWarnings("rawtypes")
    protected Map getStormConf()
    {
        return this.stormConf;
    }

    /**
     * Get Topology context.
     *
     * @return Topology context
     */
    protected TopologyContext getContext()
    {
        return this.context;
    }

    /**
     * Get SpoutOutputCollector.
     *
     * @return SpoutOutputCollector
     */
    protected SpoutOutputCollector getCollector()
    {
        return this.collector;
    }
}
