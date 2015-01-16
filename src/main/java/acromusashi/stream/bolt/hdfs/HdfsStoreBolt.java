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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.MessageBolt;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.InitFailException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

/**
 * 受信したメッセージをHDFSに出力するBolt<br/>
 * 
 * @author kimura
 */
public class HdfsStoreBolt extends MessageBolt
{
    /** serialVersionUID */
    private static final long            serialVersionUID = -2877852415844943739L;

    /** logger */
    private static final Logger          logger           = LoggerFactory.getLogger(HdfsStoreBolt.class);

    /** HDFSへの出力コンポーネント */
    private transient HdfsOutputSwitcher delegate         = null;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public HdfsStoreBolt()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);

        String componentId = context.getThisComponentId();
        int taskId = context.getThisTaskId();

        HdfsStoreConfig config = new HdfsStoreConfig();

        config.setOutputUri((String) stormConf.get("hdfsstorebolt.outputuri"));
        config.setFileNameHeader((String) stormConf.get("hdfsstorebolt.filenameheader"));
        config.setFileSwitchIntarval(((Long) stormConf.get("hdfsstorebolt.interval")).intValue());
        config.setFileNameBody("_" + componentId + "_" + taskId + "_");

        boolean isPreprocess = true;
        Object isPreprocessObj = stormConf.get("hdfsstorebolt.executepreprocess");
        if (isPreprocessObj != null && isPreprocessObj instanceof Boolean)
        {
            isPreprocess = ((Boolean) isPreprocessObj).booleanValue();
        }

        try
        {
            // HDFSファイルシステム取得
            Configuration conf = new Configuration();
            Path dstPath = new Path(config.getOutputUri());
            FileSystem fileSystem = dstPath.getFileSystem(conf);

            // HDFSに対する前処理実施。一時ファイルを本ファイルにリネームする。
            if (isPreprocess)
            {
                HdfsPreProcessor.execute(fileSystem, config.getOutputUri(),
                        config.getFileNameHeader() + config.getFileNameBody(),
                        config.getTmpFileSuffix());
            }

            this.delegate = new HdfsOutputSwitcher();
            this.delegate.initialize(fileSystem, config, System.currentTimeMillis());
        }
        catch (Exception ex)
        {
            logger.warn("Failed to HDFS write initialize.", ex);
            throw new InitFailException(ex);
        }
    }

    @Override
    public void onMessage(StreamMessage message) throws Exception
    {
        this.delegate.appendLine(message.toString(), System.currentTimeMillis());
    }

    @Override
    public void cleanup()
    {
        // cleanupメソッドはLocalClusterでしか呼ばれないため注意
        logger.info("HDFSSinkBolt Cleanup Start.");

        try
        {
            this.delegate.close();
        }
        catch (IOException ex)
        {
            logger.warn("Failed to HDFS write close. Skip close.", ex);
        }

        logger.info("HDFSSinkBolt Cleanup finished.");
    }
}
