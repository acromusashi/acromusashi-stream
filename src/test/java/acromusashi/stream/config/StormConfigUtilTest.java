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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import backtype.storm.Config;

/**
 * StormConfigUtilのテストクラス
 * 
 * @author kimura
 */
public class StormConfigUtilTest
{
    /** 試験用データファイル配置ディレクトリ*/
    private static final String DATA_DIR = "src/test/resources/"
                                                 + StringUtils.replaceChars(
                                                         StormConfigUtilTest.class.getPackage().getName(),
                                                         '.', '/') + '/';

    /**
     * String形式の設定値を取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getStringValue(Config, String, String)}
     * @test String形式の設定値を取得できること
     *    condition::  String形式の設定値に対応したKeyを指定
     *    result:: String形式の設定値を取得できること
     */
    @Test
    public void testGetStringValue_String値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        String actual = StormConfigUtil.getStringValue(conf, "CassandraSpout.field", "");

        // 検証
        assertEquals("word", actual);
    }

    /**
     * String形式の設定値が未存在時、デフォルト値を取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getStringValue(Config, String, String)}
     * @test String形式の設定値が未存在時、デフォルト値を取得できること
     *    condition::  String形式の設定値が存在しないKeyを指定
     *    result:: String形式の設定値が未存在時、デフォルト値を取得できること
     */
    @Test
    public void testGetStringValue_String値未存在時デフォルト値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        String actual = StormConfigUtil.getStringValue(conf, "CassandraSpout.NotFound", "default");

        // 検証
        assertEquals("default", actual);
    }

    /**
     * Int形式の設定値を取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getIntValue(Config, String, int)}
     * @test Int形式の設定値を取得できること
     *    condition::  Int形式の設定値に対応したKeyを指定
     *    result:: Int形式の設定値を取得できること
     */
    @Test
    public void testGetIntValue_Int値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        int actual = StormConfigUtil.getIntValue(conf, "topology.workers", 10);

        // 検証
        assertEquals(4, actual);
    }

    /**
     * Int形式の設定値が未存在時、デフォルト値を取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getIntValue(Config, String, int)}
     * @test Int形式の設定値が未存在時、デフォルト値を取得できること
     *    condition::  Int形式の設定値が存在しないKeyを指定
     *    result:: Int形式の設定値が未存在時、デフォルト値を取得できること
     */
    @Test
    public void testGetIntValue_Int値未存在時デフォルト値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        int actual = StormConfigUtil.getIntValue(conf, "topology.workers.NotFound", 10);

        // 検証
        assertEquals(10, actual);
    }

    /**
     * List形式の設定値を取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getStringListValue(Config, String)}
     * @test List形式の設定値を取得できること
     *    condition::  List形式の設定値に対応したKeyを指定
     *    result:: List形式の設定値を取得できること
     */
    @Test
    public void testGetStringListValue_List値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        List<String> actual = StormConfigUtil.getStringListValue(conf, "casa.word");

        // 検証
        assertEquals(3, actual.size());
        assertEquals("a", actual.get(0));
        assertEquals("b", actual.get(1));
        assertEquals("c", actual.get(2));
    }

    /**
     * List形式の設定値が未存在時、空リストを取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getStringListValue(Config, String)}
     * @test List形式の設定値が未存在時、空リストを取得できること
     *    condition::  List形式の設定値が存在しないKeyを指定
     *    result:: List形式の設定値が未存在時、空リストを取得できること
     */
    @Test
    public void testGetStringListValue_List値未存在時デフォルト値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        List<String> actual = StormConfigUtil.getStringListValue(conf, "casa.word.NotFound");

        // 検証
        assertEquals(0, actual.size());
    }

    /**
     * Map形式の設定値を取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getMapValue(Config, String)}
     * @test Map形式の設定値を取得できること
     *    condition::  Map形式の設定値に対応したKeyを指定
     *    result:: Map形式の設定値を取得できること
     */
    @SuppressWarnings("rawtypes")
    @Test
    public void testGetStringMapValue_Map値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        Map actual = StormConfigUtil.getMapValue(conf, "casa");

        // 検証
        assertEquals(3, actual.size());
        assertEquals(actual.get("title"), "Test Frame");
        assertEquals(actual.get("width"), "ff");
        assertEquals(actual.get("height"), "rr");
    }

    /**
     * Map形式の設定値が未存在時、空リストを取得できることを確認する。
     * 
     * @target {@link StormConfigUtil#getMapValue(Config, String)}
     * @test Map形式の設定値が未存在時、nullを取得できること
     *    condition::  Map形式の設定値が存在しないKeyを指定
     *    result:: Map形式の設定値が未存在時、nullを取得できること
     */
    @SuppressWarnings("rawtypes")
    @Test
    public void testGetStringListValue_Map値未存在時デフォルト値取得確認() throws IOException
    {
        // 準備
        Config conf = StormConfigGenerator.loadStormConfig(DATA_DIR
                + "StormConfigUtilTest_FileReadConfirm.yaml");

        // 実施
        Map actual = StormConfigUtil.getMapValue(conf, "casa.NotFound");

        // 検証
        assertNull(actual);
    }
}
