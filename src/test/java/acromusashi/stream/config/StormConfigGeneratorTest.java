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

import java.io.IOException;

import org.junit.Test;

import acromusashi.stream.util.ResourceResolver;
import backtype.storm.Config;

/**
 * StormConfigGeneratorのテストクラス
 * 
 * @author kimura
 */
public class StormConfigGeneratorTest
{
    /**
     * ファイルパスを指定し、設定オブジェクトを取得できることを確認する。
     * 
     * @target {@link StormConfigGenerator#loadStormConfig(String)}
     * @test 設定オブジェクトを取得できること
     *    condition::  yamlファイルのパスを指定
     *    result:: 設定オブジェクトを取得できること
     */
    @Test
    public void testLoadStormConfig_読込成功() throws IOException
    {
        // 実施
        Config actual = StormConfigGenerator.loadStormConfig(ResourceResolver.resolve("StormConfigGeneratorTest_testLoadStormConfig_ReadSuccess.yaml"));

        // 検証
        assertEquals("192.168.100.100", actual.get("nimbus.host"));
        assertEquals(Integer.valueOf(6627), actual.get("nimbus.thrift.port"));
        assertEquals(Boolean.valueOf(false), actual.get("topology.debug"));
    }

    /**
     * yamlファイルが存在しないファイルパスを指定して読み込みを行った際、例外が発生することを確認する。
     * 
     * @target {@link StormConfigGenerator#loadStormConfig(String)}
     * @test 例外が発生すること
     *    condition::  yamlファイルが存在しないファイルパスを指定
     *    result:: 例外が発生すること
     */
    @Test(expected = IOException.class)
    public void testLoadStormConfig_ファイル未存在() throws IOException
    {
        // 実施
        StormConfigGenerator.loadStormConfig("StormConfigGeneratorTest_testLoadStormConfig_NotFound.yaml");
    }
}
