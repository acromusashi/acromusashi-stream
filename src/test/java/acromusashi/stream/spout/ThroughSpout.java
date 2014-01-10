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

import java.util.List;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * 指定したフィールド指定を保持するStorm-UnitTest用Spout
 */
public class ThroughSpout extends BaseConfigurationSpout
{

    /** serialVersionUID */
    private static final long serialVersionUID = 1668791100773315754L;

    /** 下流に転送するフィールドリスト */
    private List<String>      fields;

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
    public void nextTuple()
    {
        // StormのUnitTestでは呼ばれないため
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
