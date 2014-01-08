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
package acromusashi.stream.component.kestrel.spout;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Kestrelからメッセージを取得した際の情報を保持するエンティティクラス
 * 
 * @author kimura
 */
public class KestrelSourceId
{
    /** Kestrelクライアント接続情報インデックス */
    private int  index;

    /** Kestrelから情報を取得した際のTxId */
    private long id;

    /**
     * インデックス値、TxIdを指定してインスタンスを生成する。
     * 
     * @param index インデックス値
     * @param id TxId
     */
    public KestrelSourceId(int index, long id)
    {
        this.index = index;
        this.id = id;
    }

    /**
     * @return the index
     */
    public int getIndex()
    {
        return this.index;
    }

    /**
     * @return the id
     */
    public long getId()
    {
        return this.id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String result = ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        return result;
    }
}
