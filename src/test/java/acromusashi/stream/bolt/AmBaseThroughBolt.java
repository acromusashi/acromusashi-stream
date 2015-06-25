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

import java.util.List;
import java.util.Map;

import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.TopologyContext;

/**
 * 受信したTupleに対して指定したフィールドを抽出して下流に流すStorm-UnitTest用Bolt<br>
 *
 * @author kimura
 */
public class AmBaseThroughBolt extends AmBaseBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 1668791100773315754L;

    /** StreamMessageにつめるメッセージフィールドリスト */
    private List<String>      fields;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public AmBaseThroughBolt()
    {}

    @SuppressWarnings("rawtypes")
    @Override
    public void onPrepare(Map stormConf, TopologyContext context)
    {
        // 何もしない
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
    public void onExecute(StreamMessage input)
    {
        StreamMessage result = new StreamMessage();
        result.setHeader(input.getHeader());
        for (String field : this.fields)
        {
            result.addField(field, input.getField(field));
        }

        emit(result, "KeyHistory");
    }
}
