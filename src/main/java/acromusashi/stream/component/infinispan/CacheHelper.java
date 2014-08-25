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
package acromusashi.stream.component.infinispan;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

/**
 * Infinispanのキャッシュをラッピングするヘルパークラス
 *
 * @author kimura
 *
 * @param <K> InfinispanCacheKeyの型
 * @param <V> InfinispanCacheValueの型
 */
public class CacheHelper<K, V>
{
    /** キャッシュサーバURL */
    protected String             cacheServerUrl;

    /** キャッシュ名称 */
    protected String             cacheName;

    /** Remoteキャッシュマネージャ */
    protected RemoteCacheManager remoteCacheManager;

    /** 最新データを保持するキャッシュ */
    protected RemoteCache<K, V>  remoteCache;

    /**
     * 接続情報を指定してインスタンスを生成する。
     *
     * @param cacheServerUrl キャッシュサーバURL
     * @param cacheName キャッシュ名称
     */
    public CacheHelper(String cacheServerUrl, String cacheName)
    {
        this.cacheServerUrl = cacheServerUrl;
        this.cacheName = cacheName;
    }

    /**
     * キャッシュを初期化する。
     */
    public void initCache()
    {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Configuration config = builder.classLoader(loader).addServers(this.cacheServerUrl).build();

        this.remoteCacheManager = new RemoteCacheManager(config, true);
        this.remoteCache = this.remoteCacheManager.getCache(this.cacheName, true);
    }

    /**
     * キャッシュを取得する。
     *
     * @return キャッシュ
     */
    public RemoteCache<K, V> getCache()
    {
        return this.remoteCache;
    }
}
