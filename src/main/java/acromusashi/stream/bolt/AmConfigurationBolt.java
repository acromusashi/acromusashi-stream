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

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * BaseBolt, this class has the following values ​​in the field, <br>
 * and auto extract {@link acromusashi.stream.entity.StreamMessage} from {@link backtype.storm.tuple.Tuple}.
 *
 * <ol>
 * <li>Storm configuration</li>
 * <li>Topology context</li>
 * <li>SpoutOutputCollector</li>
 * </ol>
 */
public abstract class AmConfigurationBolt extends BaseRichBolt
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

    /** Executing Tuple */
    private Tuple             executingTuple;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        // extract message from Tuple
        this.executingTuple = input;

        StreamMessage message = null;
        boolean messageGet = false;
        if (input.contains(FieldName.MESSAGE_VALUE) == true)
        {
            Object obj = input.getValueByField(FieldName.MESSAGE_VALUE);
            if (obj instanceof StreamMessage)
            {
                message = (StreamMessage) obj;
                messageGet = true;
            }
        }

        if (message == null)
        {
            message = new StreamMessage();
        }

        for (String field : input.getFields())
        {
            if (messageGet && FieldName.MESSAGE_VALUE.equals(field))
            {
                continue;
            }

            Object obj = input.getValueByField(field);
            message.addField(field, obj);
        }

        try
        {
            onMessage(message);
        }
        finally
        {
            this.executingTuple = null;
        }
    }

    /**
     * Execute procedure when receive message.
     *
     * @param message Received message
     */
    public abstract void onMessage(StreamMessage message);

    /**
     * Notify ack for inputed tuple.
     */
    protected void ack()
    {
        if (this.executingTuple != null)
        {
            getCollector().ack(this.executingTuple);
        }
    }

    /**
     * Notify fail for inputed tuple.
     */
    protected void fail()
    {
        if (this.executingTuple != null)
        {
            getCollector().fail(this.executingTuple);
        }
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

    /**
     * Get executingTuple.
     *
     * @return executingTuple
     */
    protected Tuple getExecutingTuple()
    {
        return this.executingTuple;
    }
}
