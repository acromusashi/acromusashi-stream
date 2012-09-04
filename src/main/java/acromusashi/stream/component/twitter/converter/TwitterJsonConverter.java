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
package acromusashi.stream.component.twitter.converter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import net.sf.json.JSONObject;
import acromusashi.stream.converter.AbstractMessageConverter;
import acromusashi.stream.entity.Header;
import acromusashi.stream.exception.ConvertFailException;

/**
 * JSON形式のTweetを共通メッセージに変換するコンバータ。<br/>
 * 
 * @author kimura
 */
public class TwitterJsonConverter extends AbstractMessageConverter
{
    /** serialVersionUID */
    private static final long  serialVersionUID = 2781560994999751987L;

    /** 保持されるタイムスタンプのDateFormat */
    public static final String DATE_FORMAT      = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";

    /**
     * デフォルトコンストラクタ
     */
    public TwitterJsonConverter()
    {}

    @Override
    public String getType()
    {
        return "twitterjson";
    }

    @Override
    public Header createHeader(Object input) throws ConvertFailException
    {
        JSONObject twitterJson = JSONObject.fromObject(input);

        String dateStr = twitterJson.getString("created_at");
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT,
                Locale.ENGLISH);
        Date created = null;

        try
        {
            created = dateFormat.parse(dateStr);
        }
        catch (ParseException pex)
        {
            throw new ConvertFailException(pex);
        }

        Header header = new Header();
        header.setTimestamp(created.getTime());
        header.setMessageId(twitterJson.getString("id"));
        // ツイートしたクライアント名をSourceに設定
        header.setSource(twitterJson.getString("source"));
        header.setType(getType());

        return header;
    }
}
