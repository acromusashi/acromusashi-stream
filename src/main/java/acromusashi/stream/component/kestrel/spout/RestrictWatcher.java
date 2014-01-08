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

import java.io.File;

import org.apache.commons.lang.StringUtils;

/**
 * KestrelSpoutの規制状態を管理するクラス。<br/>
 * 指定されたパスを基に規制ファイルの存在を確認し、規制の有無を返す。
 * 
 * @author kimura
 */
public class RestrictWatcher
{
    /** 規制ファイル */
    protected File targetFile;

    /**
     * 規制ファイルのパスを指定してオブジェクトを生成する。
     * 
     * @param filePath 規制ファイルのパス
     */
    public RestrictWatcher(String filePath)
    {
        if (StringUtils.isEmpty(filePath) == false)
        {
            this.targetFile = new File(filePath);
        }
    }

    /**
     * 規制の有無を確認する。
     * 
     * @return 「規制あり」の場合true、「規制なし」の場合false
     */
    public boolean isRestrict()
    {
        if (this.targetFile == null)
        {
            return false;
        }

        return this.targetFile.exists();
    }
}
