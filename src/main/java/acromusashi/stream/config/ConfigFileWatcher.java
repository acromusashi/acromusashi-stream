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
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config file watch class.
 * 
 * @author kimura
 */
public class ConfigFileWatcher
{
    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(ConfigFileWatcher.class);

    /** Target file */
    protected File              targetFile;

    /** Target file watch interval. */
    protected long              watchInterval;

    /** Target file last watch time. */
    protected long              lastWatchTime;

    /** Target file last modify time. */
    protected long              lastModifytime;

    /**
     * Constructs instance with watch target path.
     *
     * @param targetPath watch target path
     * @param watchIntervalSec watch interval sec
     */
    public ConfigFileWatcher(String targetPath, long watchIntervalSec)
    {
        this.targetFile = new File(targetPath);
        this.watchInterval = TimeUnit.SECONDS.toMillis(watchIntervalSec);
    }

    /**
     * Initialize object.
     */
    public void init()
    {
        this.lastWatchTime = getNowTime();

        if (this.targetFile.exists())
        {
            this.lastModifytime = this.targetFile.lastModified();
        }
    }

    /**
     * Read config file if fulfill following conditions.<br>
     * <ol>
     * <li>(nowtime - lastWatchTime) > watchInterval</li>
     * <li>(target file's lastmodifytime) > lastModifytime</li>
     * </ol>
     * 
     * @return config read result if updated.
     * @throws IOException If read failed.
     */
    public Map<String, Object> readIfUpdated() throws IOException
    {
        long nowTime = getNowTime();
        if ((nowTime - this.lastWatchTime) <= this.watchInterval)
        {
            return null;
        }

        this.lastWatchTime = nowTime;

        if (this.targetFile.exists() == false)
        {
            return null;
        }

        long targetFileModified = this.targetFile.lastModified();
        if (this.lastModifytime >= targetFileModified)
        {
            return null;
        }

        this.lastModifytime = targetFileModified;

        return StormConfigGenerator.readYaml(this.targetFile);
    }

    /**
     * Get now time.
     * 
     * @return now time
     */
    protected long getNowTime()
    {
        return System.currentTimeMillis();
    }
}
