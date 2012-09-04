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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 切替インターバルを受け取り、整時用の基準時刻の生成/切替タイミングの生成を行うユーティリティクラス<br/>
 * 例として、8:35を初期化時刻とし、10分を切替インターバルとして受け取った場合、ファイル名称用の時刻として8:30を返す。<br/>
 * 
 * @author kimura
 */
public class TimeIntervalFormatUtil
{
    /**
     * デフォルトコンストラクタ(インスタンス化防止用)
     */
    private TimeIntervalFormatUtil()
    {}

    /**
     * 切替インターバルの値が閾値内に収まっているかの確認を行う。<br/>
     * <br/>
     * 切替インターバル　1<= interval <= 100<br/>
     * 切替単位 MillSecond、Second、Minute、Hourのいずれか<br/>
     * 
     * @param interval 切替インターバル
     * @param unit 切替単位
     * @return 切替インターバルの値が閾値内に収まっているか
     */
    public static boolean checkValidInterval(int interval, TimeUnit unit)
    {
        if (0 >= interval || 100 < interval)
        {
            return false;
        }

        switch (unit)
        {
        case HOURS:
            break;
        case MINUTES:
            break;
        case SECONDS:
            break;
        case MILLISECONDS:
            break;
        default:
            return false;
        }

        return true;
    }

    /**
     * 指定した初期化時刻と切替単位を基に、整時用の基準時刻(long値)を取得する。
     * 
     * @param initializeTime 初期化時刻
     * @param intarval 切替インターバル
     * @param intervalUnit 切替単位
     * @return 整時用の基準時刻(long値)
     * @throws ParseException パース失敗時
     */
    public static long generateInitialBaseTime(long initializeTime,
            int intarval, TimeUnit intervalUnit) throws ParseException
    {
        // １つ大きな単位をベースに基準時刻を取得する。
        // 例：10分間隔をインターバルとし、8:35を起動時刻とした場合、8:00を基準時刻とする。
        long datumTime = generateDatumTime(initializeTime, intervalUnit);

        // 基準時刻から初期生成ファイル名称用の日時を算出する。
        // 例：10分間隔をインターバルとし、8:35を起動時刻とした場合、8:00を基準時刻とし、
        //    そこから8:10、8:20・・・とずらしていって初期生成ファイル名称用の日時(8:30)を算出する。
        long initialBaseTime = generateNextFileBaseTime(initializeTime,
                datumTime, intarval, intervalUnit);

        return initialBaseTime;
    }

    /**
     * 指定した初期化時刻と切替単位を基に、整時用の基準時刻(long値)を取得する。
     * 
     * @param initializeTime 初期化時刻
     * @param unit 切替単位
     * @return 整時用の基準時刻(long値)
     * @throws ParseException パース失敗時
     */
    public static long generateDatumTime(long initializeTime, TimeUnit unit)
            throws ParseException
    {
        // 初期に生成するファイル名称を基準となる値にするため、ファイル切替インターバル（単位）より１つ大きな単位を取得し
        // １つ大きな単位をベースに時刻を算出する。
        // 例：10分間隔であれば「0.10.20....」というのが基準となる値。
        SimpleDateFormat parentDateFormat = new SimpleDateFormat(
                TimeUnitUtil.getDatePattern(TimeUnitUtil.getParentUnit(unit)));

        // １つ大きな単位をベースに基準時刻を取得し、初期生成ファイル名称用の日時を算出する。
        // 例：10分間隔をインターバルとし、8:35を起動時刻とした場合、8:00を基準時刻とする。
        String datumTimeString = parentDateFormat.format(new Date(
                initializeTime));

        long datumTime = parentDateFormat.parse(datumTimeString).getTime();
        return datumTime;
    }

    /**
     * ファイル切替時に「次のファイル名称の基準時刻」を生成する。
     * 
     * @param nowTime ファイル切替発生時刻
     * @param oldSwitchTime 前回の切り替え閾値時刻
     * @param intarval ファイル切替インターバル
     * @param intervalUnit ファイル切替インターバル（単位）
     * @return 次のファイル名称のベース時刻
     */
    public static long generateNextFileBaseTime(long nowTime,
            long oldSwitchTime, int intarval, TimeUnit intervalUnit)
    {
        long nextBaseTime = oldSwitchTime;

        while (nowTime > nextBaseTime + intervalUnit.toMillis(intarval))
        {
            nextBaseTime += intervalUnit.toMillis(intarval);
        }

        return nextBaseTime;
    }
}
