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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * TimeUnitから該当のファイル名パターンを取得するユーティリティクラス
 * 
 * @author kimura
 */
public class TimeUnitUtil
{
    /** パターンマップ */
    private static Map<TimeUnit, String> formatMap__;

    /**
     * デフォルトコンストラクタ（インスタンス化防止用）
     */
    private TimeUnitUtil()
    {}

    static
    {
        Map<TimeUnit, String> baseMap = new HashMap<TimeUnit, String>();
        baseMap.put(TimeUnit.DAYS, "yyyyMMdd");
        baseMap.put(TimeUnit.HOURS, "yyyyMMddHH");
        baseMap.put(TimeUnit.MINUTES, "yyyyMMddHHmm");
        baseMap.put(TimeUnit.SECONDS, "yyyyMMddHHmmss");
        baseMap.put(TimeUnit.MILLISECONDS, "yyyyMMddHHmmssSSS");
        formatMap__ = Collections.unmodifiableMap(baseMap);
    }

    /**
     * TimeUnitに対応した日付パターンフォーマットを取得する
     * 
     * @param unit 時間単位
     * @return 対応フォーマット
     */
    public static String getDatePattern(TimeUnit unit)
    {
        return formatMap__.get(unit);
    }

    /**
     * 指定したTimeUnitより１つ大きい単位のTimeUnitを取得する。
     * 
     * @param unit 時間単位
     * @return １つ大きい単位のTimeUnit
     */
    public static TimeUnit getParentUnit(TimeUnit unit)
    {
        TimeUnit result = null;

        switch (unit)
        {
        case HOURS:
            result = TimeUnit.DAYS;
            break;
        case MINUTES:
            result = TimeUnit.HOURS;
            break;
        case SECONDS:
            result = TimeUnit.MINUTES;
            break;
        case MILLISECONDS:
            result = TimeUnit.SECONDS;
            break;
        case MICROSECONDS:
            result = TimeUnit.MILLISECONDS;
            break;
        case NANOSECONDS:
            result = TimeUnit.MICROSECONDS;
            break;
        default:
            break;
        }

        return result;
    }
}
