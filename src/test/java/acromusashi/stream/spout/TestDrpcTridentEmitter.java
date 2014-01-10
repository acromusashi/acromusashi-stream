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

import storm.trident.operation.TridentCollector;
import backtype.storm.task.TopologyContext;

/**
 * DrpcTridentEmitterテスト用の拡張クラス
 * 
 * @author kimura
 */
public class TestDrpcTridentEmitter extends DrpcTridentEmitter
{
    /**
     * 何もしない
     */
    @SuppressWarnings("rawtypes")
    @Override
    protected void prepare(Map conf, TopologyContext context)
    {
        // Do nothing.
    }

    /**
     * 何もしない
     */
    @Override
    protected void emitTuples(String funcArgs, TridentCollector collector)
    {
        // Do nothing.
    }
}
