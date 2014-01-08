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

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * MQとの通信を行う。
 * 
 * @author otoda
 */
public interface RabbitmqClient
{
    /**
     * MQへデータを格納する。
     * 
     * @param queueName キュー名
     * @param message 格納するデータ
     * @throws RabbitmqCommunicateException コネクションの取得に失敗した場合
     */
    void send(String queueName, Object message) throws RabbitmqCommunicateException;

    /**
     * MQからデータを取得する。
     * 
     * @param queueName キュー名
     * @return 取得したデータ
     * @throws RabbitmqCommunicateException コネクションの取得に失敗した場合
     */
    Object receive(String queueName) throws RabbitmqCommunicateException;

    /**
     * MQが保持しているメッセージ数を取得する。
     * 
     * @param queueName
     *            キュー名
     * @return キューに格納しているメッセージ数
     * @throws RabbitmqCommunicateException
     *             コネクションの取得に失敗した場合
     */
    int getQueueSize(String queueName) throws RabbitmqCommunicateException;
}
