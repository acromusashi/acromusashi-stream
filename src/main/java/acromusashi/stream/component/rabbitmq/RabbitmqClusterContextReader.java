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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * 定義ファイルを読み込む。
 * 
 * @author otoda
 */
public class RabbitmqClusterContextReader implements ApplicationContextAware
{
    /** Springコンテキスト */
    private ApplicationContext context;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public RabbitmqClusterContextReader()
    {}

    /**
     * 定義ファイルを読み込む。
     * 
     * @return 定義ファイルから読み込んだ情報
     * @throws RabbitmqCommunicateException 定義ファイルの読み込みに失敗した場合
     */
    public List<RabbitmqClusterContext> readConfiguration() throws RabbitmqCommunicateException
    {
        Map<String, RabbitmqClusterContext> rabbitmqClusterContextMap = null;
        try
        {
            rabbitmqClusterContextMap = this.context.getBeansOfType(RabbitmqClusterContext.class);
        }
        catch (Exception ex)
        {
            String message = "RabbitmqClusterContext Bean get failed.";
            throw new RabbitmqCommunicateException(message, ex);
        }

        // RabbitMQクラスタ構成コンテキストが取得できなかった場合は例外を出力する。
        if (rabbitmqClusterContextMap == null || rabbitmqClusterContextMap.isEmpty())
        {
            String message = "RabbitmqClusterContext Bean get failed.";
            throw new RabbitmqCommunicateException(message);
        }

        Collection<RabbitmqClusterContext> rabbitmqClusterContexts = rabbitmqClusterContextMap.values();
        return new ArrayList<RabbitmqClusterContext>(rabbitmqClusterContexts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException
    {
        this.context = applicationContext;
    }
}
