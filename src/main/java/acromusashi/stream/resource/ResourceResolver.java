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
package acromusashi.stream.resource;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Resource path resolve utility class.
 *
 * @author kimura
 */
public class ResourceResolver
{
    /**
     * Private default constructor
     */
    private ResourceResolver()
    {}

    /**
     * Return target file path from class and resource's path.
     *
     * @param clazz Target class
     * @param path resource's path
     * @return File object
     */
    public static File resolve(Class<?> clazz, String path)
    {
        URL url = clazz.getResource(path);
        if (url == null)
        {
            return null;
        }

        File result;
        try
        {
            result = Paths.get(url.toURI()).toFile();
        }
        catch (URISyntaxException ex)
        {
            return null;
        }

        return result;
    }

    /**
     * Return target file path from resource's path.
     *
     * @param path resource's path
     * @return File object
     */
    public static File resolve(String path)
    {
        Class<?> callerClass = resolveCaller();
        URL url = callerClass.getResource(path);
        if (url == null)
        {
            return null;
        }

        File result;
        try
        {
            result = Paths.get(url.toURI()).toFile();
        }
        catch (URISyntaxException ex)
        {
            return null;
        }

        return result;
    }

    /**
     * Return target file's content from resource's path.<br>
     * Resource is used caller class.
     *
     * @param path resource's path
     * @return read result
     */
    public static String readResource(String path)
    {
        Class<?> callerClass = resolveCaller();
        if (callerClass == null)
        {
            return null;
        }

        return readResource(resolveCaller(), path);
    }

    /**
     * Return target file's content from class and resource's path.<br>
     *
     * @param clazz Target class
     * @param path resource's path
     * @return read result
     */
    public static String readResource(Class<?> clazz, String path)
    {
        File targetFile = resolve(clazz, path);
        if (targetFile == null)
        {
            return null;
        }

        String result;

        try
        {
            result = FileUtils.readFileToString(targetFile, "UTF-8");
        }
        catch (IOException ex)
        {
            return null;
        }

        return result;
    }

    /**
     * Return target file's content by list from class and resource's path.<br>
     * Resource is used caller class.
     *
     * @param path resource's path
     * @return read result list
     */
    public static List<String> readListResource(String path)
    {
        Class<?> callerClass = resolveCaller();
        if (callerClass == null)
        {
            return null;
        }

        return readListResource(resolveCaller(), path);
    }

    /**
     * Return target file's content by list from class and resource's path.<br>
     *
     * @param clazz Target class
     * @param path resource's path
     * @return read result list
     */
    public static List<String> readListResource(Class<?> clazz, String path)
    {
        File targetFile = resolve(clazz, path);
        if (targetFile == null)
        {
            return null;
        }

        List<String> result;
        try
        {
            result = FileUtils.readLines(targetFile, "UTF-8");
        }
        catch (IOException ex)
        {
            return null;
        }

        return result;
    }

    /**
     * Return caller class calling this class's method.
     *
     * @return Caller class
     */
    public static Class<?> resolveCaller()
    {
        Class<?> callerClass = null;

        StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
        int stackSize = stackTraces.length;

        // StackTrace is below. So check from 3rd element.
        // 1st:java.lang.Thread
        // 2nd:ResourceResolver#resolveCaller(this method)
        for (int stackIndex = 2; stackIndex < stackSize; stackIndex++)
        {
            StackTraceElement stackTrace = stackTraces[stackIndex];
            String callerClassName = stackTrace.getClassName();
            if (StringUtils.equals(ResourceResolver.class.getName(), callerClassName) == false)
            {
                try
                {
                    callerClass = Class.forName(callerClassName);
                    break;
                }
                catch (ClassNotFoundException ex)
                {
                    return null;
                }
            }
        }

        return callerClass;
    }
}
