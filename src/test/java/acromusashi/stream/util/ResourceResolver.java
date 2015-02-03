package acromusashi.stream.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * テスト用ファイルのリソースを解決するユーティリティクラス
 *
 * @author kimura
 */
public class ResourceResolver
{
    /**
     * クラスとパスを指定し、テストデータ用のパスを返す。
     *
     * @param clazz 対象リソース
     * @param path リソース上のパス
     * @return ファイル
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
     * パスを指定し、テストデータ用のパスを返す。
     *
     * @param clazz 対象リソース
     * @param path リソース上のパス
     * @return ファイル
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
     * パスを指定して特定したテストデータの内容を文字列形式で返す。<br>
     * リソースは呼び出し元のクラスが使用される。
     *
     * @param path リソース上のパス
     * @return 読込結果
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
     * クラスとパスを指定して特定したテストデータの内容を文字列形式で返す。
     *
     * @param clazz 対象リソース
     * @param path リソース上のパス
     * @return 読込結果
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
     * パスを指定して特定したテストデータの内容を文字列リスト形式で返す。<br>
     * リソースは呼び出し元のクラスが使用される。
     *
     * @param path リソース上のパス
     * @return 読込結果
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
     * クラスとパスを指定して特定したテストデータの内容を文字列リスト形式で返す。
     *
     * @param clazz 対象リソース
     * @param path リソース上のパス
     * @return 読込結果
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
     * 本クラスのメソッドを呼び出した呼び出し元クラスを返す。
     *
     * @return 呼び出し元クラス
     */
    public static Class<?> resolveCaller()
    {
        Class<?> callerClass = null;

        StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
        int stackSize = stackTraces.length;

        // StackTrace下記の内容となっているため、3要素目から確認する。
        // 1要素目:java.lang.Thread
        // 2要素目:ResourceResolver#resolveCaller(本メソッド自身)
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
