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
import org.yaml.snakeyaml.scanner.ScannerException;

import backtype.storm.Config;

/**
 * Utility class for converting specified yaml file to Storm config object.
 * 
 * @author kimura
 */
public class StormConfigGenerator
{
    /** Config key for initial config path */
    public static final String INIT_CONFIG_KEY = "topology.init.config.path";

    /**
     * Constructor for preventing create instance.
     */
    private StormConfigGenerator()
    {}

    /**
     * Generate storm config object from the yaml file at specified path.
     * 
     * @param filePath target file path
     * @return Storm config object
     * @throws IOException Fail read yaml file or convert to storm config object.
     */
    public static Config loadStormConfig(String filePath) throws IOException
    {
        Map<String, Object> yamlConfig = readYaml(filePath);
        Config stormConf = convertYamlToStormConf(yamlConfig);

        // For track update config, has init file path.
        if (stormConf.containsKey(INIT_CONFIG_KEY) == false)
        {
            File file = new File(filePath);
            String absolutePath = file.getAbsolutePath();
            stormConf.put(INIT_CONFIG_KEY, absolutePath);
        }

        return stormConf;
    }

    /**
     * Generate storm config object from the yaml file at specified file.
     * 
     * @param targetFile target file
     * @return Storm config object
     * @throws IOException Fail read yaml file or convert to storm config object.
     */
    public static Config loadStormConfig(File targetFile) throws IOException
    {
        Map<String, Object> yamlConfig = readYaml(targetFile);
        Config stormConf = convertYamlToStormConf(yamlConfig);

        // For track update config, has init file path.
        if (stormConf.containsKey(INIT_CONFIG_KEY) == false)
        {
            String absolutePath = targetFile.getAbsolutePath();
            stormConf.put(INIT_CONFIG_KEY, absolutePath);
        }

        return stormConf;
    }

    /**
     * Convert config read from yaml file config to storm config object.
     * 
     * @param yamlConf config read from yaml
     * @return Storm config object
     */
    public static Config convertYamlToStormConf(Map<String, Object> yamlConf)
    {
        Config stormConf = new Config();

        for (Entry<String, Object> targetConfigEntry : yamlConf.entrySet())
        {
            stormConf.put(targetConfigEntry.getKey(), targetConfigEntry.getValue());
        }

        return stormConf;
    }

    /**
     * Read yaml config object from the yaml file at specified path.
     * 
     * @param filePath target file path
     * @return config read from yaml
     * @throws IOException Fail read yaml file or convert to config object.
     */
    public static Map<String, Object> readYaml(String filePath) throws IOException
    {
        File targetFile = new File(filePath);
        Map<String, Object> configObject = readYaml(targetFile);
        return configObject;
    }

    /**
     * Read yaml config object from the yaml file at specified file.
     * 
     * @param targetFile target file
     * @return config read from yaml
     * @throws IOException Fail read yaml file or convert to config object.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> readYaml(File targetFile) throws IOException
    {
        Map<String, Object> configObject = null;
        Yaml yaml = new Yaml();

        InputStream inputStream = null;
        InputStreamReader steamReader = null;

        try
        {
            inputStream = new FileInputStream(targetFile);
            steamReader = new InputStreamReader(inputStream, "UTF-8");
            configObject = (Map<String, Object>) yaml.load(steamReader);
        }
        catch (ScannerException ex)
        {
            // ScannerException/IOException are occured.
            // throw IOException because handling is same.
            throw new IOException(ex);
        }
        finally
        {
            IOUtils.closeQuietly(inputStream);
        }

        return configObject;
    }
}
