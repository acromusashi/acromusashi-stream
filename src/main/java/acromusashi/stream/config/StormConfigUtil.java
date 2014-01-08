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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;

/**
 * Storm設定オブジェクトから値を取得するユーティリティクラス
 * 
 * @author kimura
 */
public class StormConfigUtil
{
    /**
     * インスタンス化を防止するためのコンストラクタ
     */
    private StormConfigUtil()
    {}

    /**
     * Storm設定オブジェクトから文字列型の設定値を取得する。<br/>
     * 存在しない場合はデフォルト値を返す。
     * 
     * @param stormConf Storm設定オブジェクト
     * @param key 設定値のキー
     * @param defaultValue デフォルト値
     * @return 個別設定値(String型)
     */
    public static String getStringValue(Config stormConf, String key, String defaultValue)
    {
        Object config = stormConf.get(key);

        if (config == null)
        {
            return defaultValue;
        }

        return config.toString();
    }

    /**
     * Storm設定オブジェクトから文字列型の設定値を取得する。<br/>
     * 存在しない場合はデフォルト値を返す。
     * 
     * @param stormConf Storm設定オブジェクト
     * @param key 設定値のキー
     * @param defaultValue デフォルト値
     * @return 個別設定値(int型)
     */
    public static int getIntValue(Config stormConf, String key, int defaultValue)
    {
        Object config = stormConf.get(key);

        if (config == null)
        {
            return defaultValue;
        }

        return Integer.valueOf(config.toString());
    }

    /**
     * Storm設定オブジェクトから文字列型の設定値を取得する。<br/>
     * 存在しない場合は空リストを返す。
     * 
     * @param stormConf Storm設定オブジェクト
     * @param key 設定値のキー
     * @return 個別設定値(List<String>型)
     */
    @SuppressWarnings("unchecked")
    public static List<String> getStringListValue(Config stormConf, String key)
    {
        List<String> configList = (List<String>) stormConf.get(key);

        if (configList == null)
        {
            configList = new ArrayList<String>();
        }

        return configList;
    }

    /**
     * Storm設定オブジェクトからMap型の設定値を取得する。<br/>
     * 存在しない場合はnullを返す。
     * 
     * @param stormConf Storm設定オブジェクト
     * @param key 設定値のキー
     * @return 個別設定値(Map型)
     */
    @SuppressWarnings({"rawtypes"})
    public static Map getMapValue(Config stormConf, String key)
    {
        Map configMap = (Map) stormConf.get(key);
        return configMap;
    }
}
