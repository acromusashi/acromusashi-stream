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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS上のファイルに対してテキストを出力するWriterクラス。<br/>
 * HDFS上のファイル1個に対して1インスタンスが対応。
 * 
 * @author kimura
 */
public class HdfsStreamWriter
{
    /** 実際に書き込みを行うWriterオブジェクト */
    private FSDataOutputStream delegateStream;

    /** 1回の書込みごとにファイルと同期するかのフラグ */
    private boolean            isFileSyncEachTime = false;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public HdfsStreamWriter()
    {}

    /**
     * 指定したHDFS上パスにあるファイルをOpenする。
     * 
     * @param filePath HDFSパス
     * @param fs ファイルシステム
     * @param isFileSyncEachTime 書き込むたびに同期するかのフラグ
     * @throws IOException Open失敗時
     */
    public void open(String filePath, FileSystem fs, boolean isFileSyncEachTime)
            throws IOException
    {
        Path dstPath = new Path(filePath);

        if (fs.exists(dstPath) == true)
        {
            this.delegateStream = fs.append(dstPath);
        }
        else
        {
            this.delegateStream = fs.create(dstPath);
        }

        this.isFileSyncEachTime = isFileSyncEachTime;
    }

    /**
     * ファイルに対してテキストを追記する。
     * 
     * @param outputStr 出力行
     * @throws IOException 追記失敗時
     */
    public void append(String outputStr) throws IOException
    {
        this.delegateStream.writeChars(outputStr);

        if (this.isFileSyncEachTime)
        {
            sync();
        }
    }

    /**
     * ファイルに対してテキストを追記し、改行する。
     * 
     * @param outputLine 出力行
     * @throws IOException 追記失敗時
     */
    public void appendLine(String outputLine) throws IOException
    {
        this.delegateStream.writeChars(outputLine);
        this.delegateStream.writeChars(System.getProperty("line.separator"));

        if (this.isFileSyncEachTime)
        {
            sync();
        }
    }

    /**
     * これまで追記したファイルをHDFS上に反映する。
     * 
     * @throws IOException
     *             反映失敗時
     */
    public void sync() throws IOException
    {
        this.delegateStream.flush();
        this.delegateStream.sync();
    }

    /**
     * データストリームをCloseする
     * 
     * @throws IOException
     *             入出力例外発生時
     */
    public void close() throws IOException
    {
        sync();
        this.delegateStream.close();
    }
}
