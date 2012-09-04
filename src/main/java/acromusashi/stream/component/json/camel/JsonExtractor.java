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
package acromusashi.stream.component.json.camel;

import java.io.IOException;
import java.net.URLDecoder;
import java.text.MessageFormat;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.camel.Exchange;
import org.apache.camel.converter.stream.InputStreamCache;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Exchangeオブジェクトに含まれるHTTP Post Resuestを取得し、第一要素のValueを抽出するクラス<br/>
 * HTTP Post Resuestとして1個のKey(任意)-Valueを持つリクエストからValueを抽出する。<br/>
 * 抽出後、isDoValidateの値に応じてJSON形式かのバリデーションを実施（JSONオブジェクトに変換して確認）
 * 
 * @author kimura
 */
public class JsonExtractor
{
    /** logger */
    private static final Logger logger             = Logger.getLogger(JsonExtractor.class);

    /** Key-Valueのセパレータ */
    private static final String KEYVALUE_SEPARATOR = "=";

    /** バリデーション実施フラグ */
    public boolean              isDoValidate;

    /**
     * デフォルトコンストラクタ
     */
    public JsonExtractor()
    {}

    /**
     * ExchangeからValueを抽出する。<br/>
     * Valueを抽出できないオブジェクトの場合はログを出力して破棄する。<br/>
     * isDoValidate==trueの場合、JSONオブジェクトに一度変換したうえで文字列出力し、もちいる。
     * 
     * @param exchange 抽出元のExchange
     */
    public void extractJson(Exchange exchange)
    {
        InputStreamCache iscache = exchange.getIn().getBody(
                InputStreamCache.class);

        String postStr = null;

        try
        {
            postStr = IOUtils.toString(iscache);
            postStr = URLDecoder.decode(postStr, "utf-8");
        }
        catch (IOException ioex)
        {
            logger.warn(
                    "Failed to extract string from Request. Dispose request.",
                    ioex);
            return;
        }

        int firstSeparatorIndex = postStr.indexOf(KEYVALUE_SEPARATOR);
        if (firstSeparatorIndex == -1)
        {
            String logFormat = "Received request is invalid. Dispose request. : Request={0}";
            String logMessage = MessageFormat.format(logFormat, postStr);
            logger.warn(logMessage);
            return;
        }

        String jsonStr = postStr.substring(firstSeparatorIndex + 1);

        if (this.isDoValidate == false)
        {
            exchange.getOut().setBody(jsonStr);
            return;
        }

        JSONObject jsonObj = null;

        try
        {
            jsonObj = JSONObject.fromObject(jsonStr);
        }
        catch (JSONException jsonex)
        {
            logger.warn("Received request is invalid. Dispose request.", jsonex);
            return;
        }

        exchange.getOut().setBody(jsonObj.toString());
    }
}
