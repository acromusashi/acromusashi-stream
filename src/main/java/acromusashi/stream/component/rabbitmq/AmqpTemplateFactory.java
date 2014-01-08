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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * キューへのコネクションを保持する。
 */
public class AmqpTemplateFactory
{

    /** キューへのコネクションを生成するために必要な情報 */
    private AbstractContextBuilder    contextBuilder;

    /** キューへのコネクション */
    private Map<String, AmqpTemplate> amqpTemplateMap = new ConcurrentHashMap<String, AmqpTemplate>();

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public AmqpTemplateFactory()
    {}

    /**
     * キューへのコネクションを生成するために必要な情報を指定してインスタンスを生成する。
     * 
     * @param contextBuilder キューへのコネクションを生成するために必要な情報
     */
    public AmqpTemplateFactory(AbstractContextBuilder contextBuilder)
    {
        setContextBuilder(contextBuilder);
    }

    /**
     * キューへのコネクションを取得する。<br>
     * もしキューへのコネクションを保持していない場合は、生成して返却する。
     * 
     * @param queueName キュー名
     * @return キューへのコネクション
     * @throws RabbitmqCommunicateException コネクション生成に失敗した場合
     */
    public AmqpTemplate getAmqpTemplate(String queueName) throws RabbitmqCommunicateException
    {
        if (queueName == null)
        {
            String message = "QueueName is not defined.";
            throw new RabbitmqCommunicateException(message);
        }

        AmqpTemplate template = this.amqpTemplateMap.get(queueName);

        if (template == null)
        {
            template = createAmqpTemplate(queueName);
            this.amqpTemplateMap.put(queueName, template);
        }
        return template;
    }

    /**
     * キューへのコネクションを生成する。
     * 
     * @param queueName キュー名
     * @return 生成したキューへのコネクション
     * @throws RabbitmqCommunicateException 指定したキューが定義ファイルに定義されていない場合
     */
    protected AmqpTemplate createAmqpTemplate(String queueName) throws RabbitmqCommunicateException
    {
        List<String> processList = getProcessList(queueName);

        // ConnectionFactoryにフェイルオーバする順序で接続先RabbitMQプロセス一覧を設定。
        ConnectionFactory connectionFactory = getCachingConnectionFactory(queueName, processList);

        // AmqpTemplate（RabbitTemplate）にConnectionFactoryとキュー名を設定。
        RabbitTemplate template = getRabbitTemplate(queueName, connectionFactory);
        return template;
    }

    /**
     * RabbitMQプロセス一覧を取得する。
     * 
     * @param queueName キュー名
     * @return RabbitMQプロセス一覧
     * @throws RabbitmqCommunicateException 指定したキューが定義ファイルに定義されていない場合
     */
    private List<String> getProcessList(String queueName) throws RabbitmqCommunicateException
    {
        List<String> processList = getContextBuilder().getProcessList(queueName);
        if (processList == null || processList.size() == 0)
        {
            String messageFmt = "QueueName's ProcessList is not defined. QueueName={0}";
            String message = MessageFormat.format(messageFmt, queueName);
            throw new RabbitmqCommunicateException(message);
        }
        return processList;
    }

    /**
     * ConnectionFactoryを取得する。
     * 
     * @param queueName キュー名
     * @param processList RabbitMQプロセス一覧
     * @return ConnectionFactory
     * @throws RabbitmqCommunicateException connectionFactoryを取得できなかった場合
     */
    private AbstractConnectionFactory getCachingConnectionFactory(String queueName,
            List<String> processList) throws RabbitmqCommunicateException
    {
        AbstractConnectionFactory connectionFactory = null;
        try
        {
            connectionFactory = (AbstractConnectionFactory) getContextBuilder().getConnectionFactory(
                    queueName);
        }
        catch (Exception ex)
        {
            throw new RabbitmqCommunicateException(ex);
        }

        String adresses = StringUtils.join(processList, ',');
        connectionFactory.setAddresses(adresses);
        return connectionFactory;
    }

    /**
     * RabbitTemplateを取得する。
     * 
     * @param queueName キュー名
     * @param connectionFactory ConnectionFactory
     * @return RabbitTemplate
     * @throws RabbitmqCommunicateException amqpTemplateを取得できなかった場合
     */
    private RabbitTemplate getRabbitTemplate(String queueName, ConnectionFactory connectionFactory)
            throws RabbitmqCommunicateException
    {
        // AmqpTemplateは使いまわされるため、ディープコピーをする。
        RabbitTemplate template = null;
        try
        {
            template = (RabbitTemplate) BeanUtils.cloneBean(getContextBuilder().getAmqpTemplate(
                    queueName));
        }
        catch (Exception ex)
        {
            throw new RabbitmqCommunicateException(ex);
        }
        template.setConnectionFactory(connectionFactory);
        template.setExchange(queueName);
        template.setQueue(queueName);
        return template;
    }

    /**
     * @return the contextBuilder
     */
    public AbstractContextBuilder getContextBuilder()
    {
        return this.contextBuilder;
    }

    /**
     * @param contextBuilder the contextBuilder to set
     */
    public void setContextBuilder(AbstractContextBuilder contextBuilder)
    {
        this.contextBuilder = contextBuilder;
    }
}
