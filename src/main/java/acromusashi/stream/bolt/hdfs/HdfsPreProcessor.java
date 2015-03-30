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
package acromusashi.stream.bolt.hdfs;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFSSink起動時の前処理を行うクラス
 * 
 * @author kimura
 */
public class HdfsPreProcessor
{
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger(HdfsOutputSwitcher.class);

    /**
     * インスタンス化を防止するためのコンストラクタ
     */
    private HdfsPreProcessor()
    {}

    /**
     * HDFSSink起動時の前処理を行う。<br>
     * 末尾に一時ファイル名称パターンを持ち、かつリネーム先にファイルが存在しない場合リネームを行う。
     * 
     * @param hdfs ファイルシステム
     * @param baseUrl ベースURL
     * @param baseName ベース名称
     * @param tmpSuffix 一時ファイル名称パターン
     */
    public static void execute(FileSystem hdfs, String baseUrl, String baseName, String tmpSuffix)
    {
        String baseRealUrl = baseUrl;

        if (baseRealUrl.endsWith("/") == false)
        {
            baseRealUrl = baseRealUrl + "/";
        }

        String targetPattern = baseRealUrl + baseName + "[0-9]*" + tmpSuffix + "*";
        Path targetPathPattern = new Path(targetPattern);

        FileStatus[] targetTmpFiles = null;

        try
        {
            targetTmpFiles = hdfs.globStatus(targetPathPattern);
        }
        catch (IOException ioex)
        {
            logger.warn("Failed to search preprocess target files. Skip preprocess.", ioex);
            return;
        }

        if (targetTmpFiles.length == 0)
        {
            String logFormat = "Preprocess target files not exist. Path={0}";
            String logMessage = MessageFormat.format(logFormat, targetPattern);
            logger.info(logMessage);
            return;
        }

        if (logger.isInfoEnabled() == true)
        {
            printTargetPathList(targetTmpFiles);
        }

        for (FileStatus targetTmpFile : targetTmpFiles)
        {
            renameTmpFile(hdfs, targetTmpFile.getPath().toString(), tmpSuffix);
        }

    }

    /**
     * 前処理対象ファイル一覧をログ出力する。
     * 
     * @param targetTmpFiles 前処理対象ファイル一覧
     */
    private static void printTargetPathList(FileStatus[] targetTmpFiles)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Preprocess target files:");
        String lineSeparator = System.getProperty("line.separator");

        for (FileStatus targetFile : targetTmpFiles)
        {
            builder.append(targetFile.getPath() + lineSeparator);
        }

        logger.info(builder.toString());
    }

    /**
     * 前処理対象ファイルをリネームする。<br>
     * リネーム先にファイルが存在していた場合はリネームをスキップする。
     * 
     * @param hdfs ファイルシステム
     * @param targetTmpPath 前処理対象ファイルパス
     * @param tmpSuffix 一時ファイル名称パターン
     */
    private static void renameTmpFile(FileSystem hdfs, String targetTmpPath, String tmpSuffix)
    {
        String basePath = extractBasePath(targetTmpPath, tmpSuffix);

        boolean isFileExists = true;

        try
        {
            isFileExists = hdfs.exists(new Path(basePath));
        }
        catch (IOException ioex)
        {
            String logFormat = "Failed to search target file exists. Skip file rename. : TargetUri={0}";
            String logMessage = MessageFormat.format(logFormat, basePath);
            logger.warn(logMessage, ioex);
            return;
        }

        if (isFileExists)
        {
            String logFormat = "File exists renamed target. Skip file rename. : BeforeUri={0} , AfterUri={1}";
            String logMessage = MessageFormat.format(logFormat, targetTmpPath, basePath);
            logger.warn(logMessage);
        }
        else
        {
            try
            {
                hdfs.rename(new Path(targetTmpPath), new Path(basePath));
            }
            catch (IOException ioex)
            {
                String logFormat = "Failed to HDFS file rename. Skip rename file and continue preprocess. : BeforeUri={0} , AfterUri={1}";
                String logMessage = MessageFormat.format(logFormat, targetTmpPath, basePath);
                logger.warn(logMessage, ioex);
            }
        }
    }

    /**
     * 一時ファイルパスからベースパスを抽出する。
     * 
     * @param targetTmpPath 一時ファイルパス
     * @param tmpSuffix 一時ファイル名称パターン
     * @return ベースパス
     */
    public static String extractBasePath(String targetTmpPath, String tmpSuffix)
    {
        int lastIndex = targetTmpPath.lastIndexOf(tmpSuffix);
        String result = targetTmpPath.substring(0, lastIndex);
        return result;
    }
}
