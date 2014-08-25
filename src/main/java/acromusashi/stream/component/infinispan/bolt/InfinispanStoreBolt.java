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
package acromusashi.stream.component.infinispan.bolt;

import java.text.MessageFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.BaseConfigurationBolt;
import acromusashi.stream.component.infinispan.CacheHelper;
import acromusashi.stream.component.infinispan.TupleCacheMapper;
import acromusashi.stream.exception.ConvertFailException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * InfinispanにTupleの内容を保存するBolt
 *
 * @author kimura
 *
 * @param <K> InfinispanCacheKeyの型
 * @param <V> InfinispanCacheValueの型
 */
public class InfinispanStoreBolt<K, V> extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long           serialVersionUID = -1793029528020234403L;

    /** logger */
    private static final Logger         logger           = LoggerFactory.getLogger(InfinispanStoreBolt.class);

    /** キャッシュサーバURL */
    protected String                    cacheServerUrl;

    /** 一時キャッシュ名称 */
    protected String                    cacheName;

    /** CacheMapper */
    protected TupleCacheMapper<K, V>      mapper;

    /** CacheHelper */
    protected transient CacheHelper<K, V> cacheHelper;

    /**
     * TupleMapperを指定してインスタンスを生成する。
     *
     * @param mapper TupleMapper
     */
    public InfinispanStoreBolt(TupleCacheMapper<K, V> mapper)
    {
        this.mapper = mapper;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);
        this.cacheHelper = new CacheHelper<K, V>(this.cacheServerUrl, this.cacheName);
        this.cacheHelper.initCache();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        // データ保存前実行処理を実行
        onStoreBefore(input);

        K storeKey = null;
        try
        {
            storeKey = this.mapper.convertToKey(input);
        }
        catch (ConvertFailException ex)
        {
            String messageFormat = "Tuple convert to key failed. Trash tuple. : InputTuple={0}";
            String errorMessage = MessageFormat.format(messageFormat, input.toString());
            logger.warn(errorMessage, ex);
            getCollector().ack(input);
            return;
        }

        V storeValue = null;
        try
        {
            storeValue = this.mapper.convertToValue(input);
        }
        catch (ConvertFailException ex)
        {
            String messageFormat = "Tuple convert to value failed. Trash tuple. : InputTuple={0}";
            String errorMessage = MessageFormat.format(messageFormat, input.toString());
            logger.warn(errorMessage, ex);
            getCollector().ack(input);
            return;
        }

        try
        {
            this.cacheHelper.getCache().put(storeKey, storeValue);
        }
        catch (Exception ex)
        {
            String messageFormat = "Cache store failed. Trash tuple. : InputTuple={0}";
            String errorMessage = MessageFormat.format(messageFormat, input.toString());
            logger.warn(errorMessage, ex);
            getCollector().ack(input);
            return;
        }

        onStoreAfter(input, storeKey, storeValue);
        getCollector().ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // 下流に送信を行わないため、未設定
    }

    /**
     * Infinispanへのデータ保存前に実行される処理。<br>
     *
     * @param input Tuple
     */
    protected void onStoreBefore(Tuple input)
    {
        // デフォルトでは何も行わない。
    }

    /**
     * Infinispanへのデータ保存後に実行される処理。<br>
     * 保存失敗した場合は実行されない。
     *
     * @param input Tuple
     * @param storedKey 保存したKey
     * @param storedValue 保存したValue
     */
    protected void onStoreAfter(Tuple input, K storedKey, V storedValue)
    {
        // デフォルトでは何も行わない。
    }
}
