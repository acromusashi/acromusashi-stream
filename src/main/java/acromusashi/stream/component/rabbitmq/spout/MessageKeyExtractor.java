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
package acromusashi.stream.component.rabbitmq.spout;

import java.io.Serializable;

import acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * メッセージキーの抽出を行うクラス。<br>
 * RabbitMqSpoutを使用する場合、本インタフェースを継承した抽出クラスを作成し、キーの抽出処理を記述すること。
 */
public interface MessageKeyExtractor extends Serializable
{
    /**
     * 指定したオブジェクトからメッセージキーの抽出を行う。
     * 
     * @param target RabbitMQから取得したオブジェクト
     * @return Object キー情報履歴に記録するメッセージキー
     * @throws RabbitmqCommunicateException メッセージキーの抽出に失敗した場合
     */
    String extractMessageKey(Object target) throws RabbitmqCommunicateException;
}
