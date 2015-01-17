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

import java.io.Serializable;

import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.ConvertFailException;

/**
 * Cacheに保存するKey、ValueをTupleから生成する変換インタフェース
 *
 * @author kimura
 *
 * @param <K> InfinispanCacheKeyの型
 * @param <V> InfinispanCacheValueの型
 */
public interface TupleCacheMapper<K, V> extends Serializable
{
    /**
     * Cacheに保存するKeyを生成する。
     *
     * @param input received message
     * @return Cacheに保存するKey
     * @throws ConvertFailException 変換失敗時
     */
    K convertToKey(StreamMessage input) throws ConvertFailException;

    /**
     * Cacheに保存するValueを生成する。
     *
     * @param input received message
     * @return Cacheに保存するValue
     * @throws ConvertFailException 変換失敗時
     */
    V convertToValue(StreamMessage input) throws ConvertFailException;
}
