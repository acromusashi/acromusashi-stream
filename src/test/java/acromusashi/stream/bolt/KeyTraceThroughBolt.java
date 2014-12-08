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

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * 受信したTupleに対して指定したフィールドを抽出して下流に流すStorm-UnitTest用Bolt<br>
 *
 * @author kimura
 */
public class KeyTraceThroughBolt extends KeyTraceBaseBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 1668791100773315754L;

    /** 下流に転送するフィールドリスト */
    private List<String>      fields;

    /** メッセージキーリスト */
    private List<String>      keys;

    /** 手動Ack設定 */
    private boolean           isManualAck;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public KeyTraceThroughBolt()
    {}

    @SuppressWarnings("rawtypes")
    @Override
    public void onPrepare(Map stormConf, TopologyContext context)
    {
        // 何もしない
    }

    @Override
    public void onExecute(Tuple input)
    {
        List<Object> tuple = new ArrayList<Object>();

        // キーが指定されていない場合は受信したパラメータをそのまま下流に流す
        if (this.keys == null)
        {
            for (String targetField : this.fields)
            {
                Object targetValue = input.getValueByField(targetField);
                tuple.add(targetValue);
            }

            emitWithNoAnchorKey(tuple);
            return;
        }

        // キーが指定されている場合は受信したパラメータにキーを付与して「キー数」だけTupleを生成して下流に流す
        for (String targetKey : this.keys)
        {
            tuple = new ArrayList<Object>();
            for (String targetField : this.fields)
            {
                Object targetValue = input.getValueByField(targetField);
                tuple.add(targetValue);
            }
            emitWithNoAnchor(tuple, targetKey);
        }

        if (this.isManualAck)
        {
            ack(input);
        }
    }

    @Override
    public List<String> getDeclareOutputFields()
    {
        return this.fields;
    }

    /**
     * @param fields the fields to set
     */
    public void setFields(List<String> fields)
    {
        this.fields = fields;
    }

    /**
     * @param keys the keys to set
     */
    public void setKeys(List<String> keys)
    {
        this.keys = keys;
    }

    /**
     * @param isManualAck the isManualAck to set
     */
    public void setManualAck(boolean isManualAck)
    {
        this.isManualAck = isManualAck;
    }
}
