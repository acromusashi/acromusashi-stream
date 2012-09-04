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

/**
 * HDFSへの出力設定を保持する設定インスタンス
 * 
 * @author kimura
 */
public class HdfsStoreConfig
{
    /**
     * デフォルトコンストラクタ
     */
    public HdfsStoreConfig()
    {}

    /** HDFS出力先Uri */
    public String   outputUri              = "";

    /** 出力ファイル名称ヘッダ */
    public String   fileNameHeader         = "HDFSStore";

    /** ファイル名ボディ。各アプリケーションにて指定すること。 */
    public String   fileNameBody           = "";

    /** 一時ファイルの末尾につくサフィックス */
    public String   tmpFileSuffix          = ".tmp";

    /** ファイル名切替インターバル */
    public int      fileSwitchIntarval     = 10;

    /** ファイル名切替インターバル（単位） */
    public TimeUnit fileSwitchIntervalUnit = TimeUnit.MINUTES;

    /** 1回の書込みごとにファイル同期するかのフラグ */
    public boolean  isFileSyncEachTime     = true;
}
