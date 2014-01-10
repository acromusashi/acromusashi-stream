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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * 定義ファイルから読み込んだ情報を保持する。
 * 
 * @author otoda
 */
public class RabbitmqClusterContext implements ApplicationContextAware
{
    /** RabbitMQプロセス一覧 */
    private List<String>        mqProcessList;

    /** キュー一覧 */
    private List<String>        queueList;

    /** 呼出元別、接続先RabbitMQプロセスの定義マップ */
    private Map<String, String> connectionProcessMap;

    /** コネクション生成クラスを示すBeanID */
    private String              connectionFactoryId;

    /** AmqpTemplate */
    private AmqpTemplate        amqpTemplate;

    /** Springコンテキスト */
    private ApplicationContext  context;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public RabbitmqClusterContext()
    {
        this.mqProcessList = new LinkedList<String>();
        this.queueList = new ArrayList<String>();
        this.connectionProcessMap = new HashMap<String, String>();
    }

    /**
     * 接続先RabbitMQプロセスがRabbitMQプロセス一覧に定義されていることを確認する。
     * 
     * @param mqProcessList RabbitMQプロセス一覧
     * @param connectionProcessMap 呼出元別、接続先RabbitMQプロセスの定義マップ
     * @throws RabbitmqCommunicateException 接続先RabbitMQプロセスがRabbitMQプロセス一覧に定義されていない場合
     */
    private void validateProcessReference(List<String> mqProcessList,
            Map<String, String> connectionProcessMap) throws RabbitmqCommunicateException
    {
        //RabbitMQプロセス一覧が未設定の場合、例外を出力する。
        if (mqProcessList == null || mqProcessList.size() == 0)
        {
            String message = "ProcessList is not defined.";
            throw new RabbitmqCommunicateException(message);
        }

        // 呼出元別、接続先RabbitMQプロセスの定義マップが未設定の場合、確認を行わない。
        if (connectionProcessMap == null || connectionProcessMap.size() == 0)
        {
            return;
        }

        Set<String> processSet = new HashSet<String>(mqProcessList);
        Set<String> connectionProcessSet = new HashSet<String>(connectionProcessMap.values());

        if (processSet.containsAll(connectionProcessSet) == false)
        {
            @SuppressWarnings("unchecked")
            Set<String> nonContainedProcessSet = new HashSet<String>(CollectionUtils.subtract(
                    connectionProcessSet, processSet));
            String messageFmt = "Connection process is illegal. NonContainedProcessSet={0}";
            String message = MessageFormat.format(messageFmt, nonContainedProcessSet.toString());
            throw new RabbitmqCommunicateException(message);
        }
    }

    /**
     * @return the processList
     */
    public List<String> getMqProcessList()
    {
        return this.mqProcessList;
    }

    /**
     * RabbitMQプロセス一覧を検証して設定する。
     * 
     * @param mqProcessList the mqProcessList to set
     * @throws RabbitmqCommunicateException RabbitMQプロセス一覧が指定されていない場合
     */
    public void setMqProcessList(List<String> mqProcessList) throws RabbitmqCommunicateException
    {
        if (mqProcessList == null || mqProcessList.size() == 0)
        {
            String message = "ProcessList is not defined.";
            throw new RabbitmqCommunicateException(message);
        }

        validateProcessReference(mqProcessList, this.connectionProcessMap);
        this.mqProcessList = mqProcessList;
    }

    /**
     * @return the queueList
     */
    public List<String> getQueueList()
    {
        return this.queueList;
    }

    /**
     * キュー一覧を検証して設定する。
     * 
     * @param queueList the queueList to set
     * @throws RabbitmqCommunicateException キュー一覧が指定されていない場合
     */
    public void setQueueList(List<String> queueList) throws RabbitmqCommunicateException
    {
        if (queueList == null || queueList.size() == 0)
        {
            String message = "QueueList is not defined.";
            throw new RabbitmqCommunicateException(message);
        }
        this.queueList = queueList;
    }

    /**
     * @return the connectionProcessMap
     */
    public Map<String, String> getConnectionProcessMap()
    {
        return this.connectionProcessMap;
    }

    /**
     * 呼出元別、接続先RabbitMQプロセスの定義マップを検証して設定する。
     * 
     * @param connectionProcessMap the connectionProcessMap to set
     * @throws RabbitmqCommunicateException 接続先RabbitMQプロセスがRabbitMQプロセス一覧に定義されていない場合
     */
    public void setConnectionProcessMap(Map<String, String> connectionProcessMap)
            throws RabbitmqCommunicateException
    {
        Map<String, String> tempConnectionProcessMap = connectionProcessMap;
        if (connectionProcessMap == null)
        {
            tempConnectionProcessMap = new HashMap<String, String>();
        }
        validateProcessReference(this.mqProcessList, tempConnectionProcessMap);
        this.connectionProcessMap = tempConnectionProcessMap;
    }

    /**
     * @return the connectionFactoryId
     */
    public ConnectionFactory getConnectionFactory()
    {
        // このメソッドはSpringにより定義されたコネクション生成クラスのインスタンスを返却する。
        // Spring定義で「scope="prototype"」とすることで、常にnewされたインスタンスを返却する。 
        return this.context.getBean(this.connectionFactoryId, ConnectionFactory.class);
    }

    /**
     * @param connectionFactoryId the connectionFactoryId to set
     */
    public void setConnectionFactoryId(String connectionFactoryId)
    {
        this.connectionFactoryId = connectionFactoryId;
    }

    /**
     * @return the amqpTemplate
     */
    public AmqpTemplate getAmqpTemplate()
    {
        return this.amqpTemplate;
    }

    /**
     * @param amqpTemplate the amqpTemplate to set
     */
    public void setAmqpTemplate(AmqpTemplate amqpTemplate)
    {
        this.amqpTemplate = amqpTemplate;
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
