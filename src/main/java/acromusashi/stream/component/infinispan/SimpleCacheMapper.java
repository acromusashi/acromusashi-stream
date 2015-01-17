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

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.ConvertFailException;

/**
 * Tuple中の指定したキーから値を取得してKey、Valueを生成するSimpleなMapper
 *
 * @author kimura
 *
 * @param <K> InfinispanCacheKeyの型
 * @param <V> InfinispanCacheValueの型
 */
public class SimpleCacheMapper<K, V> implements TupleCacheMapper<K, V>
{
    /** serialVersionUID */
    private static final long serialVersionUID = 883625725130826587L;

    /** Key用フィールドID */
    private String            keyField         = FieldName.MESSAGE_KEY;

    /** Value用フィールドID */
    private String            valueField       = FieldName.MESSAGE_VALUE;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public SimpleCacheMapper()
    {}

    /**
     * フィールドIDを指定してインスタンスを生成する。
     *
     * @param keyField Key用フィールドID
     * @param valueField Value用フィールドID
     */
    public SimpleCacheMapper(String keyField, String valueField)
    {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public K convertToKey(StreamMessage input) throws ConvertFailException
    {
        K key = null;

        try
        {
            key = (K) input.getField(this.keyField);
        }
        catch (Exception ex)
        {
            throw new ConvertFailException(ex);
        }

        return key;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public V convertToValue(StreamMessage input) throws ConvertFailException
    {
        V value = null;

        try
        {
            value = (V) input.getField(this.valueField);
        }
        catch (Exception ex)
        {
            throw new ConvertFailException(ex);
        }

        return value;
    }
}
