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

import java.io.IOException;

/**
 * RabbitMQコンポーネントにおいてエラー発生時に発生する例外クラス
 * 
 * @author otoda
 */
public class RabbitmqCommunicateException extends IOException
{
    /** serialVersionUID */
    private static final long serialVersionUID = -2575872766214401504L;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public RabbitmqCommunicateException()
    {
        super();
    }

    /**
     * メッセージを指定してインスタンスを生成する。
     * 
     * @param message メッセージ
     */
    public RabbitmqCommunicateException(String message)
    {
        super(message);
    }

    /**
     * メッセージ、発生原因例外を指定してインスタンスを生成する。
     * 
     * @param message メッセージ
     * @param cause 発生原因例外
     */
    public RabbitmqCommunicateException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * 発生原因例外を指定してインスタンスを生成する。
     * 
     * @param cause 発生原因例外
     */
    public RabbitmqCommunicateException(Throwable cause)
    {
        super(cause);
    }
}
