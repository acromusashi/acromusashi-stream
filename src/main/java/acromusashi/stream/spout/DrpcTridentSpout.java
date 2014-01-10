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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

/**
 * DRPCリクエストを受信してTridentの処理を開始するSpout
 * 
 * @author kimura
 */
public class DrpcTridentSpout<E extends DrpcTridentEmitter> implements ITridentSpout<Object>
{
    /** serialVersionUID */
    private static final long   serialVersionUID = 741250318522431506L;

    /** ロガー */
    private static final Logger logger           = LoggerFactory.getLogger(DrpcTridentSpout.class);

    /** DRPCリクエストを受信する際の指定機能名 */
    protected String            function;

    /** Tupleを送付する際のフィールド定義 */
    protected Fields            outputFields;

    /** Emitterのクラス型 */
    protected Class<E>          emitterClassType;

    /**
     * DRPCリクエスト受信機能名を指定してインスタンスを生成する。
     * 
     * @param function DRPCリクエスト受信機能名
     * @param outputFields Tupleを送付する際のフィールド定義
     * @param emitterClassType Emitterのクラス型
     */
    public DrpcTridentSpout(String function, Fields outputFields, Class<E> emitterClassType)
    {
        this.function = function;
        this.outputFields = outputFields;
        this.emitterClassType = emitterClassType;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public storm.trident.spout.ITridentSpout.BatchCoordinator<Object> getCoordinator(
            String txStateId, Map conf, TopologyContext context)
    {
        return new DrpcTridentCoodinator();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public storm.trident.spout.ITridentSpout.Emitter<Object> getEmitter(String txStateId, Map conf,
            TopologyContext context)
    {
        DrpcTridentEmitter emitter = null;

        try
        {
            emitter = this.emitterClassType.newInstance();
            emitter.initialize(conf, context, this.function);
        }
        catch (Exception ex)
        {
            logger.error("Emitter create failed.", ex);
        }

        return emitter;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Map getComponentConfiguration()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Fields getOutputFields()
    {
        return this.outputFields;
    }
}
