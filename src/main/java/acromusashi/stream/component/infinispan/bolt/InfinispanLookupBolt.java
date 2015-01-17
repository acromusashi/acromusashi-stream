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

import acromusashi.stream.bolt.AmConfigurationBolt;
import acromusashi.stream.component.infinispan.CacheHelper;
import acromusashi.stream.component.infinispan.TupleCacheMapper;
import acromusashi.stream.constants.FieldName;
import acromusashi.stream.exception.ConvertFailException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * InfinispanからTupleに指定したKeyに対応したValueを取得し、取得結果を基に処理を実施するBolt
 *
 * @author kimura
 *
 * @param <K> InfinispanCacheKeyの型
 * @param <V> InfinispanCacheValueの型
 */
public class InfinispanLookupBolt<K, V> extends AmConfigurationBolt
{
    /** serialVersionUID */
    private static final long             serialVersionUID = 9028505967740858573L;

    /** logger */
    private static final Logger           logger           = LoggerFactory.getLogger(InfinispanLookupBolt.class);

    /** キャッシュサーバURL */
    protected String                      cacheServerUrl;

    /** キャッシュ名称 */
    protected String                      cacheName;

    /** CacheMapper */
    protected TupleCacheMapper<K, V>      mapper;

    /** CacheHelper */
    protected transient CacheHelper<K, V> cacheHelper;

    /**
     * TupleMapperを指定してインスタンスを生成する。
     *
     * @param cacheServerUrl キャッシュサーバURL
     * @param cacheName キャッシュ名称
     * @param mapper TupleMapper
     */
    public InfinispanLookupBolt(String cacheServerUrl, String cacheName,
            TupleCacheMapper<K, V> mapper)
    {
        this.cacheServerUrl = cacheServerUrl;
        this.cacheName = cacheName;
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
        // データ取得前実行処理を実行
        onLookupBefore(input);

        K lookupKey = null;
        try
        {
            lookupKey = this.mapper.convertToKey(input);
        }
        catch (ConvertFailException ex)
        {
            String messageFormat = "Tuple convert to key failed. Skip lookup. : InputTuple={0}";
            String errorMessage = MessageFormat.format(messageFormat, input.toString());
            logger.warn(errorMessage, ex);
        }

        V lookupValue = null;
        if (lookupKey != null)
        {
            try
            {
                lookupValue = this.cacheHelper.getCache().get(lookupKey);
            }
            catch (Exception ex)
            {
                String messageFormat = "Cache lookup failed. Continue execute. : InputTuple={0}";
                String errorMessage = MessageFormat.format(messageFormat, input.toString());
                logger.warn(errorMessage, ex);
            }
        }

        // データ取得後実行処理を実行
        onLookupAfter(input, lookupKey, lookupValue);
        getCollector().ack(input);
    }

    /**
     * Infinispanからのデータ取得前に実行される処理。<br>
     *
     * @param input Tuple
     */
    protected void onLookupBefore(Tuple input)
    {
        // デフォルトでは何も行わない。
    }

    /**
     * Infinispanからのデータ取得後に実行される処理。
     *
     * @param input Tuple
     * @param lookupKey 取得に使用したKey
     * @param lookupValue 取得したValue(取得されなかった場合はnull)
     */
    protected void onLookupAfter(Tuple input, K lookupKey, V lookupValue)
    {
        // デフォルトでは取得した結果がnull以外の場合、下流にデータを流す。
        if (lookupValue != null)
        {
            getCollector().emit(input, new Values(lookupKey, lookupValue));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // デフォルトでは
        declarer.declare(new Fields(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
    }
}
