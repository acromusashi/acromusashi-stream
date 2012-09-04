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
package acromusashi.stream.bolt.hbase;

import java.io.Serializable;

/**
 * HBase上のCell定義
 * 
 * @author kimura
 */
public class CellDefine implements Serializable
{
    /** serialVersionUID */
    private static final long serialVersionUID = -6905624411648306810L;

    /** ColumnFamiry */
    public String             family;

    /** ColumnQualifier */
    public String             qualifier;

    /**
     * コンストラクタ
     * 
     * @param family ColumnFamiry
     * @param qualifier ColumnQualifier
     */
    public CellDefine(String family, String qualifier)
    {
        this.family = family;
        this.qualifier = qualifier;
    }
}
