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
package acromusashi.stream.config;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;

/**
 * 設定値に対するバリデーションユーティリティ
 *
 * @author kimura
 */
public class ConfigValidation
{
    /** サーバアドレスでの、IPアドレスとポート番号との区切り文字*/
    private static final String SERVER_ADDRESS_SEPARATER = ":";

    /**
     * 外部からのインスタンス化を防止するデフォルトコンストラクタ。
     */
    private ConfigValidation()
    {
        // Do nothing.
    }

    /**
     * サーバアドレスの設定値を確認する。<br>
     * 「ホスト名:ポート」という形式になっている場合、サーバアドレスとして有効と判定する。
     *
     * @param value 設定値
     * @return 有効な表記ならばtrue、その他はfalse
     */
    public static boolean isServerAddress(Object value)
    {
        //nullでないこと
        if (value == null)
        {
            return false;
        }

        // String型であること
        if (String.class.isAssignableFrom(value.getClass()) == false)
        {
            return false;
        }

        // 空文字でないこと
        String valueString = (String) value;
        if (StringUtils.isEmpty(valueString))
        {
            return false;
        }

        // :（コロン）で２つに分割できること
        String[] redisConfig = valueString.split(SERVER_ADDRESS_SEPARATER);
        if (redisConfig.length < 2)
        {
            return false;
        }

        // ポートの値がint型に変換できること
        try
        {
            Integer.valueOf(redisConfig[1]);
            return true;
        }
        catch (NumberFormatException ex)
        {
            return false;
        }
    }

    /**
     * URIで記述されたファイルパスの設定値を確認する。<br>
     * URI形式として解釈可能な形式の場合、ファイルパスとして有効と判定する。
     *
     * @param value 設定値
     * @return 有効な表記ならばtrue、その他はfalse
     */
    public static boolean isUriFilePath(Object value)
    {
        // nullでないこと
        if (value == null)
        {
            return false;
        }

        // String型であること
        if (String.class.isAssignableFrom(value.getClass()) == false)
        {
            return false;
        }

        // 空文字でないこと
        String valueString = (String) value;
        if (StringUtils.isEmpty(valueString))
        {
            return false;
        }

        // 有効なファイルパス指定であること
        try
        {
            new URI(valueString);
            return true;
        }
        catch (URISyntaxException ex)
        {
            return false;
        }
    }

    /**
     * 記述されたファイルパスの設定値を確認する。
     *
     * @param value 設定値
     * @return 有効な表記ならばtrue、その他はfalse
     */
    public static boolean isFilePath(Object value)
    {
        // nullでないこと
        if (value == null)
        {
            return false;
        }

        // String型であること
        if (String.class.isAssignableFrom(value.getClass()) == false)
        {
            return false;
        }

        // 空文字でないこと
        String valueString = (String) value;
        if (StringUtils.isEmpty(valueString))
        {
            return false;
        }

        return true;
    }
}
