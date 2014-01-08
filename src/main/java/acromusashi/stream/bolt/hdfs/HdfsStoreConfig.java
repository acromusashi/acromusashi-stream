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

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * HDFSへの出力設定を保持する設定インスタンス
 * 
 * @author kimura
 */
public class HdfsStoreConfig
{
    /** ファイル名切替インターバルデフォルト値 */
    public static final int DEFAULT_INTERVAL       = 10;

    /** HDFS出力先Uri */
    private String          outputUri              = "";

    /** 出力ファイル名称ヘッダ */
    private String          fileNameHeader         = "HDFSStore";

    /** ファイル名ボディ。各アプリケーションにて指定すること。 */
    private String          fileNameBody           = "";

    /** 一時ファイルの末尾につくサフィックス */
    private String          tmpFileSuffix          = ".tmp";

    /** ファイル名切替インターバル */
    private int             fileSwitchIntarval     = DEFAULT_INTERVAL;

    /** ファイル名切替インターバル（単位） */
    private TimeUnit        fileSwitchIntervalUnit = TimeUnit.MINUTES;

    /** 1回の書込みごとにファイル同期するかのフラグ */
    private boolean         isFileSyncEachTime     = true;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public HdfsStoreConfig()
    {}

    /**
     * @return the outputUri
     */
    public String getOutputUri()
    {
        return this.outputUri;
    }

    /**
     * @param outputUri the outputUri to set
     */
    public void setOutputUri(String outputUri)
    {
        this.outputUri = outputUri;
    }

    /**
     * @return the fileNameHeader
     */
    public String getFileNameHeader()
    {
        return this.fileNameHeader;
    }

    /**
     * @param fileNameHeader the fileNameHeader to set
     */
    public void setFileNameHeader(String fileNameHeader)
    {
        this.fileNameHeader = fileNameHeader;
    }

    /**
     * @return the fileNameBody
     */
    public String getFileNameBody()
    {
        return this.fileNameBody;
    }

    /**
     * @param fileNameBody the fileNameBody to set
     */
    public void setFileNameBody(String fileNameBody)
    {
        this.fileNameBody = fileNameBody;
    }

    /**
     * @return the tmpFileSuffix
     */
    public String getTmpFileSuffix()
    {
        return this.tmpFileSuffix;
    }

    /**
     * @param tmpFileSuffix the tmpFileSuffix to set
     */
    public void setTmpFileSuffix(String tmpFileSuffix)
    {
        this.tmpFileSuffix = tmpFileSuffix;
    }

    /**
     * @return the fileSwitchIntarval
     */
    public int getFileSwitchIntarval()
    {
        return this.fileSwitchIntarval;
    }

    /**
     * @param fileSwitchIntarval the fileSwitchIntarval to set
     */
    public void setFileSwitchIntarval(int fileSwitchIntarval)
    {
        this.fileSwitchIntarval = fileSwitchIntarval;
    }

    /**
     * @return the fileSwitchIntervalUnit
     */
    public TimeUnit getFileSwitchIntervalUnit()
    {
        return this.fileSwitchIntervalUnit;
    }

    /**
     * @param fileSwitchIntervalUnit the fileSwitchIntervalUnit to set
     */
    public void setFileSwitchIntervalUnit(TimeUnit fileSwitchIntervalUnit)
    {
        this.fileSwitchIntervalUnit = fileSwitchIntervalUnit;
    }

    /**
     * @return the isFileSyncEachTime
     */
    public boolean isFileSyncEachTime()
    {
        return this.isFileSyncEachTime;
    }

    /**
     * @param isFileSyncEachTime the isFileSyncEachTime to set
     */
    public void setFileSyncEachTime(boolean isFileSyncEachTime)
    {
        this.isFileSyncEachTime = isFileSyncEachTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String result = ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        return result;
    }
}
