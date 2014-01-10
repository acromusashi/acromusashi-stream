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

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 受信したTupleに対して指定したフィールドを抽出して下流に流すStorm-UnitTest用Bolt<br>
 */
public class ThroughBolt extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 1668791100773315754L;

    /** 下流に転送するフィールドリスト */
    private List<String>      fields;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public ThroughBolt()
    {
        // Do nothing.
    }

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

        getCollector().emit(tuple);
        getCollector().ack(input);
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
