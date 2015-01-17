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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 受信したTupleに対して指定したフィールドを抽出して下流に流すStorm-UnitTest用Bolt<br>
 */
public class ThroughBolt extends BaseRichBolt
{
    /** serialVersionUID */
    private static final long         serialVersionUID = 1668791100773315754L;

    /** 下流に転送するフィールドリスト */
    private List<String>              fields;

    /** Output Collector */
    private transient OutputCollector collector;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public ThroughBolt()
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        List<Object> tuple = new ArrayList<Object>();

        // 受信したパラメータをそのまま下流に流す
        for (String targetField : this.fields)
        {
            Object targetValue = input.getValueByField(targetField);
            tuple.add(targetValue);
        }

        this.collector.emit(tuple);
        this.collector.ack(input);
    }

    /**
     * @param fields the fields to set
     */
    public void setFields(List<String> fields)
    {
        this.fields = fields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(this.fields.toArray(new String[0])));
    }

}
