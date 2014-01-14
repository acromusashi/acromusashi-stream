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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

/**
 * JsonValueExtractorのテストクラス
 * 
 * @author kimura
 */
public class JsonValueExtractorTest
{
    /** 試験用データファイル配置ディレクトリ*/
    private static final String DATA_DIR = "src/test/resources/"
                                                 + StringUtils.replaceChars(
                                                         JsonValueExtractorTest.class.getPackage().getName(),
                                                         '.', '/') + '/';

    /**
     * JSONから対象要素が抽出可能であることを確認する。
     * 
     * @target {@link JsonValueExtractor#extractValue(Object, String, String)}
     * @test JSONから対象要素が抽出可能であること
     *    condition:: 要素を保持するJSON要素を指定して対象メソッドを実行
     *    result:: JSONから対象要素が抽出可能であること
     */
    @Test
    public void testExtractValue_抽出確認() throws IOException
    {
        // 準備
        String jsonStr = FileUtils.readFileToString(new File(DATA_DIR
                + "JsonValueExtractorTest_testExtractValue_ExtractConfirm.txt"));

        // 実施
        String actual = JsonValueExtractor.extractValue(jsonStr, "header", "messageId");

        // 検証
        assertEquals("192.168.100.31_20130419182101127_0019182101", actual);
    }
}
