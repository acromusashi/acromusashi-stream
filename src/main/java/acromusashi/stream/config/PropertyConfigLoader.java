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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.guava.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load configs from property files.
 * 
 * @author kimura
 */
public class PropertyConfigLoader
{
    /** Default charset */
    private static final String            DEFAULT_CHARSET = "UTF-8";

    /** logger */
    private static final Logger            logger          = LoggerFactory.getLogger(PropertyConfigLoader.class);

    /** Map has configs loaded from property files */
    private static Map<String, Properties> propertiesMap   = Maps.newHashMap();

    /**
     * Constructor for prevent create instance.
     */
    private PropertyConfigLoader()
    {}

    /**
    * Get from config defined property files in classpath.
    * 
    * @param filePath property file path in classpath
    * @param configKey configkey
    * @return config value defined in property file
    */
    public static Object get(String filePath, String configKey)
    {
        if (propertiesMap.containsKey(filePath) == false)
        {
            loadProperty(filePath);
        }

        Properties properties = propertiesMap.get(filePath);
        if (properties == null)
        {
            return null;
        }

        Object resultConfig = properties.getProperty(configKey);
        return resultConfig;
    }

    /**
     * Load property file.
     * 
     * @param filePath property file path in classpath
     */
    private static void loadProperty(String filePath)
    {
        try (InputStream propertyStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(
                filePath);
                Reader propertyReader = new InputStreamReader(propertyStream, DEFAULT_CHARSET);)
        {
            Properties properties = new Properties();
            properties.load(propertyReader);
            propertiesMap.put(filePath, properties);
        }
        catch (Exception ex)
        {
            String errorPattern = "Property file load failed. Skip load. : PropertyFile={0}";
            logger.warn(MessageFormat.format(errorPattern, filePath), ex);
        }
    }
}
