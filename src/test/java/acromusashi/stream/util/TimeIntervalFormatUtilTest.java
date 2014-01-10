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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * TimeIntervalFormatUtilのテストクラス
 * 
 * @author kimura
 */
public class TimeIntervalFormatUtilTest
{
    /**
     * インターバルとして正常な各パターンに対する検証を行う。
     * 
     * @target {@link TimeIntervalFormatUtil#checkValidInterval(int, TimeUnit)}
     * @test 指定した各種パターンが正常として判定されること
     *    condition:: インターバルとして正常な各パターンを指定してcheckValidIntervalメソッドを実行
     *    result:: 指定した各種パターンが正常として判定されること
     */
    @Test
    public void testCheckValidInterval_Validパターン確認()
    {
        // intervalを固定、Unitを変動させて確認
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.HOURS));
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.MINUTES));
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.SECONDS));
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.MILLISECONDS));

        // Unitを固定、intervalを変動させて確認
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(1, TimeUnit.MINUTES));
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(20, TimeUnit.MINUTES));
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(50, TimeUnit.MINUTES));
        assertTrue(TimeIntervalFormatUtil.checkValidInterval(100, TimeUnit.MINUTES));
    }

    /**
     * インターバルとして異常な各パターンに対する検証を行う。
     * 
     * @target {@link TimeIntervalFormatUtil#checkValidInterval(int, TimeUnit)}
     * @test 指定した各種パターンが異常として判定されること
     *    condition:: インターバルとして異常な各パターンを指定してcheckValidIntervalメソッドを実行
     *    result:: 指定した各種パターンが異常として判定されること
     */
    @Test
    public void testCheckValidInterval_InValidパターン確認()
    {
        // intervalを固定、Unitを変動させて確認
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.DAYS));
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.MICROSECONDS));
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(10, TimeUnit.NANOSECONDS));

        // Unitを固定、intervalを変動させて確認
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(-1, TimeUnit.MINUTES));
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(0, TimeUnit.MINUTES));
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(101, TimeUnit.MINUTES));
        assertFalse(TimeIntervalFormatUtil.checkValidInterval(102, TimeUnit.MINUTES));
    }

    /**
     * 10分インターバルを指定して基準時刻の生成を行う。
     * 
     * @target {@link TimeIntervalFormatUtil#generateInitialBaseTime(long, int, TimeUnit)
     * @test 基準時刻の生成が行われること。
     *    condition:: 10分インターバルを指定してgenerateInitialBaseTimeメソッドを実行
     *    result:: 基準時刻の生成が行われること。
     */
    @Test
    public void testGenerateInitialBaseTime_10分インターバル() throws ParseException
    {
        // 準備
        // パターン1 「現在時刻 - 基準時刻」がインターバルの3倍超過
        String target1 = "20131030163811";
        String expected1 = "20131030163000";
        // パターン2 「現在時刻 - 基準時刻」がインターバルの2倍超過
        String target2 = "20131030162811";
        String expected2 = "20131030162000";
        // パターン3 「現在時刻 - 基準時刻」がインターバルの1倍超過
        String target3 = "20131030161811";
        String expected3 = "20131030161000";
        // パターン4 「現在時刻 - 基準時刻」がインターバル未満
        String target4 = "20131030160811";
        String expected4 = "20131030160000";
        // パターン5 「現在時刻 - 基準時刻」が0
        String target5 = "20131030161000";
        String expected5 = "20131030160000";

        // Unitを固定、intervalを変動させて確認
        assertEquals(convertToDateStrToMillis(expected1),
                TimeIntervalFormatUtil.generateInitialBaseTime(convertToDateStrToMillis(target1),
                        10, TimeUnit.MINUTES));
        assertEquals(convertToDateStrToMillis(expected2),
                TimeIntervalFormatUtil.generateInitialBaseTime(convertToDateStrToMillis(target2),
                        10, TimeUnit.MINUTES));
        assertEquals(convertToDateStrToMillis(expected3),
                TimeIntervalFormatUtil.generateInitialBaseTime(convertToDateStrToMillis(target3),
                        10, TimeUnit.MINUTES));
        assertEquals(convertToDateStrToMillis(expected4),
                TimeIntervalFormatUtil.generateInitialBaseTime(convertToDateStrToMillis(target4),
                        10, TimeUnit.MINUTES));
        assertEquals(convertToDateStrToMillis(expected5),
                TimeIntervalFormatUtil.generateInitialBaseTime(convertToDateStrToMillis(target5),
                        10, TimeUnit.MINUTES));
    }

    /**
     * 指定した日付文字列（パターン：「yyyyMMddHHmmss」）を基にlong形式の時刻を生成する。
     * 
     * @param dateStr 日付文字列
     * @return long形式の時刻
     * @throws ParseException パース失敗時
     */
    private long convertToDateStrToMillis(String dateStr) throws ParseException
    {
        String dateFormatStr = "yyyyMMddHHmmss";
        DateFormat dateFormat = new SimpleDateFormat(dateFormatStr);
        long result = dateFormat.parse(dateStr).getTime();
        return result;
    }
}
