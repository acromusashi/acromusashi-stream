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
package acromusashi.stream.camel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.spring.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Camelの初期化を行うクラス
 * 
 * @author kimura
 */
public class CamelInitializer
{
    /** 初期化タイムアウト時間 */
    private static final int                     INITIALIZE_TIMEOUT = 60;

    /** logger */
    private static final Logger                  logger             = LoggerFactory.getLogger(CamelInitializer.class);

    /** 初期化済みのProducerTemplateを保持するMap */
    private static Map<Object, ProducerTemplate> templates          = new HashMap<Object, ProducerTemplate>();

    /**
     * インスタンス化を防止するためのコンストラクタ
     */
    private CamelInitializer()
    {}

    /**
     * Camelを初期化し、送信オブジェクトを取得する。
     * 初期化済みのコンテキストパスを指定した場合、
     * 再度初期化せずに初期化済みのProducerTemplateを返す。
     * 
     * @param contextUri コンテキストパス
     * @return Camelへの送信オブジェクト
     * @throws Exception 初期化失敗時
     */
    public static ProducerTemplate generateTemplete(String contextUri) throws Exception
    {
        String initKey = contextUri.intern();
        synchronized (initKey)
        {
            ProducerTemplate template = templates.get(initKey);

            // 初期化済みの場合は、初期化済みのProducerTemplateを返す。
            if (template != null)
            {
                return template;
            }

            // 初期化を行う
            Main main = new Main();
            main.setApplicationContextUri(contextUri);
            main.enableHangupSupport();

            // run Camel
            CamelInitializeThread initializeThread = new CamelInitializeThread(main);
            initializeThread.start();

            int timeCounter = 0;

            // keep ProducerTemplate
            while (!main.isStarted())
            {
                try
                {
                    TimeUnit.SECONDS.sleep(1);
                }
                catch (InterruptedException ex)
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Occur interrupt. Ignore interrupt.", ex);
                    }
                }

                timeCounter++;

                if (INITIALIZE_TIMEOUT < timeCounter)
                {
                    logger.error("Timed out to camel initialization. Stop to application start.");
                    throw new Exception("Camel initialization time out.");
                }
            }

            // ProducerTemplateを取得する
            template = main.getCamelTemplate();
            // 初期化済みのProducerTemplateを保持する
            templates.put(initKey, template);

            return template;
        }
    }

    /**
     * Camelの初期化を行うスレッド
     * 
     * @author kimura
     */
    private static class CamelInitializeThread extends Thread
    {
        /** Camelを管理するクラス */
        private Main main;

        /**
         * インスタンスを生成する。
         * 
         * @param main
         *            Camelを管理するクラス
         */
        public CamelInitializeThread(Main main)
        {
            this.main = main;
        }

        /**
         * Camelを初期化し、動作を開始する。
         */
        @Override
        public void run()
        {
            try
            {
                this.main.run();
            }
            catch (Exception ex)
            {
                logger.error("Failure to run Camel.", ex);
            }
        }
    }
}
