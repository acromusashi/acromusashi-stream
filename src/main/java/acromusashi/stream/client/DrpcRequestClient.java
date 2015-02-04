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
package acromusashi.stream.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DRPCClient;

/**
 * DRPCServerに対してアクセスを行う汎用クライアント
 * 
 * @author kimura
 */
public class DrpcRequestClient
{
    /** デフォルトポート */
    private static final int    DEFAULT_PORT    = 3772;

    /** デフォルトタイムアウト */
    private static final int    DEFAULT_TIMEOUT = 30000;

    /** logger */
    private static final Logger logger          = LoggerFactory.getLogger(DrpcRequestClient.class);

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public DrpcRequestClient()
    {}

    /**
     * プログラムエントリポイント<br/>
     * <br/>
     * 下記の引数/オプションを使用する。<br/>
     * <ul>
     * <li>-h DRPCServerホスト(必須入力）</li>
     * <li>-p DRPCServerポート(任意指定 デフォルト3772)</li>
     * <li>-t DRPCタイムアウト(ミリ秒単位)(任意指定 デフォルト30000)</li>
     * <li>-f DRPC実行機能(function)(必須入力)</li>
     * <li>-s DRPC実行引数(必須入力)</li>
     * <li>-sh ヘルプ表示</li>
     * </ul>
     * 
     * @param args 起動引数
     */
    public static void main(String... args)
    {
        DrpcRequestClient client = new DrpcRequestClient();
        client.startSendRequest(args);
    }

    /**
     * DRPCリクエスト送信を開始する。
     * 
     * @param args 起動時引数
     */
    public void startSendRequest(String... args)
    {
        Options cliOptions = createOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
        HelpFormatter help = new HelpFormatter();

        try
        {
            commandLine = parser.parse(cliOptions, args);
        }
        catch (ParseException pex)
        {
            help.printHelp(DrpcRequestClient.class.getName(), cliOptions, true);
            return;
        }

        if (commandLine.hasOption("sh"))
        {
            // ヘルプオプションが指定されていた場合にはヘルプを表示して終了
            help.printHelp(DrpcRequestClient.class.getName(), cliOptions, true);
            return;
        }

        // コマンドラインから設定値を取得
        String drpcHost = commandLine.getOptionValue("h");
        String function = commandLine.getOptionValue("f");
        String funcArg = commandLine.getOptionValue("a");

        int drpcPort = DEFAULT_PORT;
        if (commandLine.hasOption("p") == true)
        {
            drpcPort = Integer.parseInt(commandLine.getOptionValue("p"));
        }

        int timeout = DEFAULT_TIMEOUT;
        if (commandLine.hasOption("t") == true)
        {
            timeout = Integer.parseInt(commandLine.getOptionValue("t"));
        }

        sendRequest(drpcHost, drpcPort, timeout, function, funcArg);
    }

    /**
     * DRPCリクエストを送信する。
     * 
     * @param host DRPCServerホスト
     * @param port DRPCServerポート
     * @param timeout DRPCタイムアウト(ミリ秒単位)
     * @param func DRPC実行機能(function)
     * @param funcArg DRPC実行引数
     */
    public void sendRequest(String host, int port, int timeout, String func, String funcArg)
    {
        DRPCClient client = null;

        try
        {
            client = createClient(host, port, timeout);
        }
        catch (Exception ex)
        {
            logger.error("DRPCClient connect failed.", ex);
            return;
        }

        String result = null;

        try
        {
            result = client.execute(func, funcArg);
            logger.info("DRPCRequest result is " + result);
        }
        catch (Exception ex)
        {
            logger.error("DRPCRequest failed.", ex);
        }
        finally
        {
            client.close();
        }
    }

    /**
     * DRPCClientを生成する。
     * 
     * @param host DRPCServerホスト
     * @param port DRPCServerポート
     * @param timeout DRPCタイムアウト(ミリ秒単位)
     * @return DRPCClient
     */
    protected DRPCClient createClient(String host, int port, int timeout)
    {
        return new DRPCClient(host, port, timeout);
    }

    /**
     * コマンドライン解析用のオプションを生成
     * 
     * @return コマンドライン解析用のオプション
     */
    public static Options createOptions()
    {
        Options cliOptions = new Options();

        // DRPCServer Hostオプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("DRPCServer Host");
        OptionBuilder.withDescription("DRPCServer Host");
        OptionBuilder.isRequired(true);
        Option serverOption = OptionBuilder.create("h");

        // DRPCServer Portオプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("DRPCServer Port");
        OptionBuilder.withDescription("DRPCServer Port");
        OptionBuilder.isRequired(false);
        Option portOption = OptionBuilder.create("p");

        // タイムアウト指定オプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("DRPC Request Timeout");
        OptionBuilder.withDescription("DRPC Request Timeout");
        OptionBuilder.isRequired(false);
        Option timeoutOption = OptionBuilder.create("t");

        // DRPC Function指定オプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("DRPC Function");
        OptionBuilder.withDescription("DRPC Function");
        OptionBuilder.isRequired(true);
        Option functionOption = OptionBuilder.create("f");

        // DRPC Function Arg指定オプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("DRPC Function Arg");
        OptionBuilder.withDescription("DRPC Function Arg");
        OptionBuilder.isRequired(true);
        Option funcArgOption = OptionBuilder.create("a");

        // ヘルプオプション
        OptionBuilder.withDescription("show help");
        Option helpOption = OptionBuilder.create("sh");

        cliOptions.addOption(serverOption);
        cliOptions.addOption(portOption);
        cliOptions.addOption(timeoutOption);
        cliOptions.addOption(functionOption);
        cliOptions.addOption(funcArgOption);
        cliOptions.addOption(helpOption);
        return cliOptions;
    }
}
