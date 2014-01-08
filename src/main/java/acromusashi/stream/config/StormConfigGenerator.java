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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;

/**
 * 指定されたYamlファイルをベースにStorm設定オブジェクトを生成するユーティリティクラス
 * 
 * @author kimura
 */
public class StormConfigGenerator
{
    /**
     * インスタンス化を防止するためのコンストラクタ
     */
    private StormConfigGenerator()
    {}

    /**
     * 指定されたパスに存在するYamlファイルを読み込み、Storm設定オブジェクトを生成する。
     * 
     * @param filePath 読込先ファイルパス
     * @return Storm設定オブジェクト
     * @throws IOException 入出力例外発生時
     */
    public static Config loadStormConfig(String filePath) throws IOException
    {
        Map<String, Object> yamlConfig = readYaml(filePath);

        Config stormConf = convertYamlToStormConf(yamlConfig);
        return stormConf;
    }

    /**
     * Yamlファイルから読み込んだ設定値をStorm設定オブジェクトに変換する。
     * 
     * @param yamlConf Yamlファイルから読み込んだ設定値
     * @return Storm設定オブジェクト
     */
    public static Config convertYamlToStormConf(Map<String, Object> yamlConf)
    {
        Config stormConf = new Config();

        for (Entry<String, Object> targetConfigEntry : yamlConf.entrySet())
        {
            stormConf.put(targetConfigEntry.getKey(),
                    targetConfigEntry.getValue());
        }

        return stormConf;
    }

    /**
     * ファイルパスで指定された設定ファイル（Yaml形式）を読み込み、Yaml設定値オブジェクトを返す。
     * 
     * @param filePath 読込先ファイルパス
     * @return 設定ファイルを読みこんだ設定値オブジェクト
     * @throws IOException 入出力例外発生時
     */
    @SuppressWarnings(
    { "unchecked" })
    public static Map<String, Object> readYaml(String filePath)
            throws IOException
    {
        Map<String, Object> configObject = null;
        Yaml yaml = new Yaml();

        InputStream inputStream = null;
        InputStreamReader steamReader = null;
        File file = new File(filePath);

        try
        {
            inputStream = new FileInputStream(file.getAbsolutePath());
            steamReader = new InputStreamReader(inputStream, "UTF-8");
            configObject = (Map<String, Object>) yaml.load(steamReader);
        }
        catch (Exception ex)
        {
            // ScannerException/IOExceptionが発生する可能性がある。
            // 2つの例外間に継承関係がなく、ハンドリングが同じのためExceptionでキャッチしている。
            throw new IOException(ex);
        }
        finally
        {
            IOUtils.closeQuietly(inputStream);
        }

        return configObject;
    }

}
