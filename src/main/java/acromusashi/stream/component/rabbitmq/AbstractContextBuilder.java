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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * キューへのコネクションを生成するために必要な情報を保持する。
 */
public abstract class AbstractContextBuilder
{
    /** キューを保持するクラスタの構成定義*/
    protected Map<String, RabbitmqClusterContext> contextMap;

    /** キューを保持するクラスタのRabbitMQプロセス一覧 (フェイルオーバで接続する順番にした状態）*/
    protected Map<String, List<String>>           processLists;

    /**
     * クラスタ構成定義一覧を指定してインスタンスを生成する。
     * 
     * @param contextList クラスタ構成定義一覧
     * @throws RabbitmqCommunicateException 定義したキュー名が一意でない場合
     */
    public AbstractContextBuilder(List<RabbitmqClusterContext> contextList)
            throws RabbitmqCommunicateException
    {
        this.contextMap = initContextMap(contextList);
        this.processLists = initProcessLists(contextList);
    }

    /**
     * 呼出元名を取得する。
     * 
     * @param queueName キュー名
     * @return 呼出元名
     * @throws RabbitmqCommunicateException 呼出元名を取得できなかった場合
     */
    public abstract String getClientId(String queueName) throws RabbitmqCommunicateException;

    /**
     * キューを保持するクラスタの構成定義のMapを初期化する。
     * 
     * @param contextList クラスタ構成定義一覧
     * @return キューを保持するクラスタの構成定義のMap
     * @throws RabbitmqCommunicateException 定義したキュー名が一意でない場合
     */
    protected Map<String, RabbitmqClusterContext> initContextMap(
            List<RabbitmqClusterContext> contextList) throws RabbitmqCommunicateException
    {
        if (contextList.size() == 0)
        {
            String message = "RabbitMqClusterContext is empty.";
            throw new RabbitmqCommunicateException(message);
        }

        Map<String, RabbitmqClusterContext> contextMap = new HashMap<String, RabbitmqClusterContext>();

        for (RabbitmqClusterContext context : contextList)
        {
            for (String queueName : context.getQueueList())
            {
                //定義したキュー名が一意でない場合、エラーを出力する。
                if (contextMap.containsKey(queueName))
                {
                    String messageFmt = "QueueName is not unique. QueueName={0}";
                    String message = MessageFormat.format(messageFmt, queueName);
                    throw new RabbitmqCommunicateException(message);
                }

                contextMap.put(queueName, context);
            }
        }
        return contextMap;
    }

    /**
     * フェイルオーバで接続する順番にした状態の、キューを保持するクラスタのRabbitMQプロセス一覧を生成する。
     * 
     * @param contextList クラスタ構成定義一覧
     * @return RabbitMQプロセス一覧
     * @throws RabbitmqCommunicateException 定義したキュー名が一意でない場合
     */
    protected Map<String, List<String>> initProcessLists(List<RabbitmqClusterContext> contextList)
            throws RabbitmqCommunicateException
    {
        if (this.contextMap == null)
        {
            this.contextMap = initContextMap(contextList);
        }

        Map<String, List<String>> processLists = new HashMap<String, List<String>>();

        LinkedList<String> processList = null;
        String connectionProcess = null;
        int processIndex = 0;

        for (String queueName : this.contextMap.keySet())
        {
            //エンティティ内のインスタンスを変更しないよう、ディープコピーする。
            RabbitmqClusterContext context = this.contextMap.get(queueName);
            processList = new LinkedList<String>(context.getMqProcessList());

            //接続先RabbitMQプロセスが何番目に定義されているかを確認する。
            //デフォルトでは先頭(0)
            processIndex = 0;
            connectionProcess = context.getConnectionProcessMap().get(getClientId(queueName));
            if (connectionProcess != null)
            {
                processIndex = processList.indexOf(connectionProcess);
            }

            //接続先RabbitMQプロセスが先頭となるよう、RabbitMQプロセス一覧を並び替える。
            LinkedList<String> backwardProcesses = new LinkedList<String>(processList.subList(0,
                    processIndex));
            LinkedList<String> forwardProcesses = new LinkedList<String>(processList.subList(
                    processIndex, processList.size()));
            forwardProcesses.addAll(backwardProcesses);
            processList = new LinkedList<String>(forwardProcesses);

            processLists.put(queueName, processList);
        }

        return processLists;
    }

    /**
     * キューを保持するクラスタのRabbitMQプロセス一覧を取得する。
     * 
     * @param queueName キュー名
     * @return RabbitMQプロセス一覧
     */
    public List<String> getProcessList(String queueName)
    {
        // このメソッドを呼び出す前に、
        // this#initProcessLists()を利用してprocessListsを初期しておくこと。 
        if (this.processLists == null)
        {
            return null;
        }
        return getProcessLists().get(queueName);
    }

    /**
     * キューを保持するクラスタのConnectionFactoryを取得する。
     * 
     * @param queueName キュー名
     * @return ConnectionFactory
     */
    public ConnectionFactory getConnectionFactory(String queueName)
    {
        // このメソッドを呼び出す前に、
        // this#initContextMap()を利用してcontextMapを初期しておくこと。 
        if (this.contextMap == null)
        {
            return null;
        }

        RabbitmqClusterContext context = this.contextMap.get(queueName);
        if (context == null)
        {
            return null;
        }

        return context.getConnectionFactory();
    }

    /**
     * キューを保持するクラスタのAmqpTemplateを取得する。
     * 
     * @param queueName キュー名
     * @return AmqpTemplate
     */
    public AmqpTemplate getAmqpTemplate(String queueName)
    {
        // このメソッドを呼び出す前に、
        // this#initContextMap()を利用してcontextMapを初期しておくこと。 
        if (this.contextMap == null)
        {
            return null;
        }

        RabbitmqClusterContext context = this.contextMap.get(queueName);
        if (context == null)
        {
            return null;
        }

        return context.getAmqpTemplate();
    }

    /**
     * @return the processLists
     */
    public Map<String, List<String>> getProcessLists()
    {
        return this.processLists;
    }

    /**
     * @return the contextMap
     */
    public Map<String, RabbitmqClusterContext> getContextMap()
    {
        return this.contextMap;
    }
}
