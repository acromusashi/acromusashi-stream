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

import java.util.List;

public class EmitItem
{
    /** Kestrelの取得元情報 */
    private KestrelSourceId sourceId;

    /** Tuple用オブジェクト */
    private List<Object>    tuple;

    /**
     * 取得元情報とTuple用オブジェクトを指定してインスタンスを生成する。
     * 
     * @param tuple 取得元情報
     * @param sourceId Tuple用オブジェクト
     */
    public EmitItem(List<Object> tuple, KestrelSourceId sourceId)
    {
        this.tuple = tuple;
        this.sourceId = sourceId;
    }

    /**
     * @return the sourceId
     */
    public KestrelSourceId getSourceId()
    {
        return this.sourceId;
    }

    /**
     * @return the tuple
     */
    public List<Object> getTuple()
    {
        return this.tuple;
    }
}
