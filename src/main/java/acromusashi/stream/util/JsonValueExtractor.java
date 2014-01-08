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
package acromusashi.stream.util;

import net.sf.json.JSONObject;

/**
 * JSONメッセージから値を抽出するユーティリティクラス
 * 
 * @author kimura
 */
public class JsonValueExtractor
{
    /**
     * インスタンス化を防止するためのコンストラクタ
     */
    private JsonValueExtractor()
    {}

    /**
     * 指定したJSONオブジェクトから「parentKey」要素中の「key」の要素を抽出する。<br/>
     * 抽出に失敗した場合は例外が発生する。
     * 
     * @param target JSONオブジェクト
     * @param parentKey JSONオブジェクト中の親キー
     * @param childKey JSONオブジェクト中の子キー
     * @return 取得結果
     */
    public static String extractValue(Object target, String parentKey, String childKey)
    {
        JSONObject jsonObj = JSONObject.fromObject(target);
        JSONObject parentObj = jsonObj.getJSONObject(parentKey);
        String value = parentObj.getString(childKey);
        return value;
    }
}
