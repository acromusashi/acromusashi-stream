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
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import acromusashi.stream.util.TimeIntervalFormatUtil;
import acromusashi.stream.util.TimeUnitUtil;

/**
 * HDFSのWriterを保持し、出力先の切り替えを行うコンポーネント。<br/>
 * シングルスレッドから呼び出すことを前提としているため、マルチスレッドから並行して書き込みを行う必要がある場合は複数のコンポーネントを用意すること。
 * 
 * @author kimura
 */
public class HdfsOutputSwitcher
{
    /** logger */
    private static final Logger logger             = Logger.getLogger(HdfsOutputSwitcher.class);

    /** 一時ファイルのインデックス最大値 */
    private static final int    TMP_MAX            = 50;

    /** HDFSファイルシステム */
    private FileSystem          fileSystem         = null;

    /** HDFS出力設定 */
    private HdfsStoreConfig     config             = null;

    /** HDFS出力先ディレクトリUri */
    public String               outputDirUri       = "";

    /** 現在出力を行っているWriterオブジェクト */
    private HdfsStreamWriter    currentWriter      = null;

    /** 現在出力を行っているファイルのベースURI */
    private String              currentOutputUri   = null;

    /** 現在出力を行っているファイルのサフィックス */
    private String              currentSuffix      = null;

    /** 次にファイル切替を行うタイミング */
    private long                nextSwitchTime     = 0;

    /** ファイル名命名用フォーマット */
    private SimpleDateFormat    dateFormat         = null;

    /** ファイル切替インターバル（値） */
    private int                 switchTimeInterval = 10;

    /** ファイル切替インターバル（単位） */
    private TimeUnit            switchTimeUnit     = TimeUnit.MINUTES;

    /**
     * デフォルトコンストラクタ
     */
    public HdfsOutputSwitcher()
    {}

    /**
     * HDFS出力切替オブジェクトの初期化を行う。
     * 
     * @param fileSystem HDFSファイルシステム
     * @param config HDFS出力設定
     * @param initializeTime 初期化時刻
     * @throws IOException 入出力エラー発生時
     * @throws ParseException パースエラー発生時
     */
    public void initialize(FileSystem fileSystem, HdfsStoreConfig config,
            long initializeTime) throws IOException, ParseException
    {
        this.fileSystem = fileSystem;
        this.config = config;

        if (this.config.outputUri.endsWith("/") == false)
        {
            this.outputDirUri = this.config.outputUri + "/";
        }
        else
        {
            this.outputDirUri = this.config.outputUri;
        }

        // ファイル切替間隔が不正の場合誤動作を誘発するため、
        // バリデーションをかけて不正な場合はログ出力を行ってデフォルト値を設定する。
        initializeIntervalConf(this.config.fileSwitchIntarval,
                this.config.fileSwitchIntervalUnit);

        // ファイル切替インターバル（単位）をベースに時刻部分のフォーマットを取得。
        this.dateFormat = new SimpleDateFormat(
                TimeUnitUtil.getDatePattern(this.switchTimeUnit));

        // ファイル名称初期化用の時刻を生成する。
        long initialBaseTime = TimeIntervalFormatUtil.generateInitialBaseTime(
                initializeTime, this.switchTimeInterval, this.switchTimeUnit);

        this.currentOutputUri = generateOutputFileBase(this.outputDirUri,
                this.config.fileNameHeader, this.config.fileNameBody,
                this.dateFormat, initialBaseTime);

        this.nextSwitchTime = initialBaseTime
                + this.switchTimeUnit.toMillis(this.switchTimeInterval);

        updateWriter();

        logger.info("HDFSOutputSwitcher initialized.");
    }

    /**
     * メッセージをHDFSに出力する。
     * 
     * @param target 出力内容
     * @param nowTime 出力時刻
     * @throws IOException 入出力エラー発生時
     */
    public void append(String target, long nowTime) throws IOException
    {
        if (this.nextSwitchTime <= nowTime)
        {
            switchWriter(nowTime);
        }

        this.currentWriter.append(target);
    }

    /**
     * メッセージをHDFSに出力し、改行する。
     * 
     * @param target 出力行
     * @param nowTime 出力時刻
     * @throws IOException 入出力エラー発生時
     */
    public void appendLine(String target, long nowTime) throws IOException
    {
        if (this.nextSwitchTime <= nowTime)
        {
            switchWriter(nowTime);
        }

        this.currentWriter.appendLine(target);
    }

    /**
     * ファイルライターを切り替える。
     * 
     * @param nowTime 切替時刻
     */
    private void switchWriter(long nowTime)
    {
        closeRenameTmp2BaseFile();

        // 次のファイル名称に用いる時刻を算出する。
        // ファイル出力の間が空いた時のために切り替えた時刻にあわせて次のファイル名用時刻を算出する。
        long nextBaseTime = TimeIntervalFormatUtil.generateNextFileBaseTime(
                nowTime, this.nextSwitchTime, this.switchTimeInterval,
                this.switchTimeUnit);

        this.currentOutputUri = generateOutputFileBase(this.outputDirUri,
                this.config.fileNameHeader, this.config.fileNameBody,
                this.dateFormat, nextBaseTime);

        this.nextSwitchTime = nextBaseTime
                + this.config.fileSwitchIntervalUnit.toMillis(this.switchTimeInterval);

        updateWriter();
    }

    /**
     * 現在使用している出力中ファイルをクローズし、一時ファイルサフィックスが無いファイル名称にリネームする。
     */
    private void closeRenameTmp2BaseFile()
    {
        try
        {
            this.currentWriter.close();
        }
        catch (IOException ex)
        {
            String logFormat = "Failed to HDFS file close. Continue file switch. : TargetUri={0}";
            String logMessage = MessageFormat.format(logFormat,
                    this.currentOutputUri + this.currentSuffix);
            logger.warn(logMessage, ex);
        }

        boolean isFileExists = true;

        try
        {
            isFileExists = this.fileSystem.exists(new Path(
                    this.currentOutputUri));
        }
        catch (IOException ioex)
        {
            String logFormat = "Failed to search target file exists. Skip file rename. : TargetUri={0}";
            String logMessage = MessageFormat.format(logFormat,
                    this.currentOutputUri);
            logger.warn(logMessage, ioex);
            return;
        }

        if (isFileExists)
        {
            String logFormat = "File exists renamed target. Skip file rename. : BeforeUri={0} , AfterUri={1}";
            String logMessage = MessageFormat.format(logFormat,
                    this.currentOutputUri + this.currentSuffix,
                    this.currentOutputUri);
            logger.warn(logMessage);
        }
        else
        {
            try
            {
                this.fileSystem.rename(new Path(this.currentOutputUri
                        + this.currentSuffix), new Path(this.currentOutputUri));
            }
            catch (IOException ex)
            {
                String logFormat = "Failed to HDFS file rename. Skip rename file. : BeforeUri={0} , AfterUri={1}";
                String logMessage = MessageFormat.format(logFormat,
                        this.currentOutputUri + this.currentSuffix,
                        this.currentOutputUri);
                logger.warn(logMessage, ex);
            }
        }

    }

    /**
     * Writerを更新する。更新失敗した場合は50回までリトライを行う。
     */
    public void updateWriter()
    {
        HdfsStreamWriter result = new HdfsStreamWriter();
        String suffix = this.config.tmpFileSuffix;
        int suffixIndex = 0;
        boolean isFileSyncEachTime = this.config.isFileSyncEachTime;
        boolean isSucceed = false;

        while (suffixIndex < TMP_MAX)
        {
            try
            {
                result.open(this.currentOutputUri + suffix, this.fileSystem,
                        isFileSyncEachTime);
                isSucceed = true;
                break;
            }
            catch (IOException ex)
            {
                String logFormat = "Failed to HDFS file open. Skip and retry next file. : TargetUri={0}";
                String logMessage = MessageFormat.format(logFormat,
                        this.currentOutputUri + suffix);
                logger.warn(logMessage, ex);
                suffixIndex++;
                suffix = this.config.tmpFileSuffix + suffixIndex;
            }
        }

        if (isSucceed)
        {
            this.currentWriter = result;
            this.currentSuffix = suffix;
        }
        else
        {
            String logFormat = "HDFS file open failure is retry overed. Skip HDFS file open. : TargetUri={0}";
            String logMessage = MessageFormat.format(logFormat,
                    this.currentOutputUri + suffix);
            logger.warn(logMessage);
        }
    }

    /**
     * 出力先の切り替えを行うコンポーネントをクローズし、一時サフィックスが無い状態にリネームする。
     * 
     * @throws IOException 入出力エラー発生時
     */
    public void close() throws IOException
    {
        closeRenameTmp2BaseFile();
        logger.info("HDFSOutputSwitcher closed.");
    }

    /**
     * ファイル切替インターバルの値が閾値内に収まっているかの確認を行う。<br/>
     * 下記の値に収まっていることの確認を行い、収まっていない場合はデフォルト値(10分)を適用する。<br/>
     * 
     * @param interval ファイル切替インターバル
     * @param unit ファイル切替単位
     */
    public void initializeIntervalConf(int interval, TimeUnit unit)
    {
        boolean isIntervalValid = TimeIntervalFormatUtil.checkValidInterval(
                interval, unit);

        if (isIntervalValid == false)
        {
            String logFormat = "File switch interval is invalid. Apply default interval 10 minutes. : Interval={0} , TimeUnit={1}";
            String logMessage = MessageFormat.format(logFormat, interval,
                    unit.toString());
            logger.warn(logMessage);
        }
        else
        {
            this.switchTimeInterval = interval;
            this.switchTimeUnit = unit;
        }
    }

    /**
     * 以下クラス未依存メソッド。他クラスで使用する場合はユーティリティメソッドとして切り出すこと。
     */

    /**
     * ベースのファイル名称を取得する。
     * 
     * @param baseDir 出力先ディレクトリパス
     * @param fileNameHeader ファイル名ヘッダ
     * @param fileNameBody ファイル名ボディ
     * @param dateFormat 日付部フォーマット
     * @param targetDate 算出時刻
     * @return ベースファイル名称
     */
    private String generateOutputFileBase(String baseDir,
            String fileNameHeader, String fileNameBody, DateFormat dateFormat,
            long targetDate)
    {
        StringBuilder baseFileNameBuilder = new StringBuilder();

        baseFileNameBuilder.append(baseDir).append(fileNameHeader).append(
                fileNameBody);
        baseFileNameBuilder.append(dateFormat.format(new Date(targetDate)));
        String result = baseFileNameBuilder.toString();

        return result;
    }
}
