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
package acromusashi.stream.component.rabbitmq;

import java.util.List;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * RabbitMQコンポーネントを初期化する。
 * 
 * @author otoda
 */
public class RabbitmqClientFactory
{
    /** SpringコンテキストファイルからRabbitMQクラスタ構成コンテキストを読み込む */
    private RabbitmqClusterContextReader reader;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public RabbitmqClientFactory()
    {}

    /**
     * RabbitmqClientを初期化する。
     * @return 初期化したインスタンス
     * @throws RabbitmqCommunicateException 初期化に失敗した場合
     */
    public RabbitmqClient createRabbitmqClient() throws RabbitmqCommunicateException
    {
        List<RabbitmqClusterContext> contextList = this.reader.readConfiguration();
        return createRabbitmqClient(contextList);
    }

    /**
     * RabbitmqClientを初期化する。
     * @param contextList クラスタ構成定義の一覧
     * @return 初期化したインスタンス
     * @throws RabbitmqCommunicateException 初期化に失敗した場合
     */
    private static RabbitmqClient createRabbitmqClient(List<RabbitmqClusterContext> contextList)
            throws RabbitmqCommunicateException
    {
        AbstractContextBuilder contextBuilder = new HostBasedContextBuilder(contextList);
        AmqpTemplateFactory factory = new AmqpTemplateFactory(contextBuilder);
        RabbitmqClient client = new DefaultRabbitmqClient(factory);

        return client;
    }

    /**
     * @return the reader
     */
    public RabbitmqClusterContextReader getReader()
    {
        return this.reader;
    }

    /**
     * @param reader the reader to set
     */
    public void setReader(RabbitmqClusterContextReader reader)
    {
        this.reader = reader;
    }

}
