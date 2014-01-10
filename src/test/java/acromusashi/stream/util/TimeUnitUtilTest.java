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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * TimeUnitUtilのテストクラス
 * 
 * @author kimura
 */
public class TimeUnitUtilTest
{
    /**
     * getDatePatternで取得されるパターンが正しいことを確認する。
     * 
     * @target {@link TimeUnitUtil#getDatePattern(java.util.concurrent.TimeUnit)}
     * @test 取得されるパターンが正しいこと
     *    condition:: 各TimeUnitを指定してgetDatePatternメソッドを実行
     *    result:: 取得されるパターンが正しいこと
     */
    @Test
    public void testGetDatePattern_各パターン確認()
    {
        assertEquals("yyyyMMdd", TimeUnitUtil.getDatePattern(TimeUnit.DAYS));
        assertEquals("yyyyMMddHH", TimeUnitUtil.getDatePattern(TimeUnit.HOURS));
        assertEquals("yyyyMMddHHmm", TimeUnitUtil.getDatePattern(TimeUnit.MINUTES));
        assertEquals("yyyyMMddHHmmss", TimeUnitUtil.getDatePattern(TimeUnit.SECONDS));
        assertEquals("yyyyMMddHHmmssSSS", TimeUnitUtil.getDatePattern(TimeUnit.MILLISECONDS));
        assertEquals(null, TimeUnitUtil.getDatePattern(TimeUnit.MICROSECONDS));
        assertEquals(null, TimeUnitUtil.getDatePattern(TimeUnit.NANOSECONDS));
    }

    /**
     * getParentUnitで取得される親TimeUnitが正しいことを確認する。
     * 
     * @target {@link TimeUnitUtil#getParentUnit(TimeUnit)}
     * @test 取得される親TimeUnitが正しいこと
     *    condition:: 各TimeUnitを指定してgetParentUnitメソッドを実行
     *    result:: 取得される親TimeUnitが正しいこと
     */
    @Test
    public void testGetParentUnit_各パターン確認()
    {
        assertEquals(null, TimeUnitUtil.getParentUnit(TimeUnit.DAYS));
        assertEquals(TimeUnit.DAYS, TimeUnitUtil.getParentUnit(TimeUnit.HOURS));
        assertEquals(TimeUnit.HOURS, TimeUnitUtil.getParentUnit(TimeUnit.MINUTES));
        assertEquals(TimeUnit.MINUTES, TimeUnitUtil.getParentUnit(TimeUnit.SECONDS));
        assertEquals(TimeUnit.SECONDS, TimeUnitUtil.getParentUnit(TimeUnit.MILLISECONDS));
        assertEquals(TimeUnit.MILLISECONDS, TimeUnitUtil.getParentUnit(TimeUnit.MICROSECONDS));
        assertEquals(TimeUnit.MICROSECONDS, TimeUnitUtil.getParentUnit(TimeUnit.NANOSECONDS));
    }
}
