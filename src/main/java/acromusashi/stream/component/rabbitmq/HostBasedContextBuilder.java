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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * RabbitMQコンポーネント<br>
 * <br>
 * {@link AbstractContextBuilder}の実装クラス。<br>
 * 呼出元識別子として「{@literal <呼出元のホスト名>_<キュー名>}」を使用する。
 */
public class HostBasedContextBuilder extends AbstractContextBuilder
{
    /** ホスト名 */
    private String hostName;

    /**
     * クラスタ構成定義一覧を指定してインスタンスを生成する。
     * 
     * @param contextList クラスタ構成定義一覧
     * @throws RabbitmqCommunicateException 以下の場合出力する。
     * <ol>
     * <li>ホスト名を取得できなかった場合</li>
     * <li>定義ファイルに誤りがある場合</li>
     * </ol>
     */
    public HostBasedContextBuilder(List<RabbitmqClusterContext> contextList)
            throws RabbitmqCommunicateException
    {
        super(contextList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientId(String queueName) throws RabbitmqCommunicateException
    {
        if (this.hostName == null)
        {
            try
            {
                this.hostName = InetAddress.getLocalHost().getHostName();
            }
            catch (UnknownHostException uhe)
            {
                String message = "Host is unresolved.";
                throw new RabbitmqCommunicateException(message, uhe);
            }
        }

        String clientId = this.hostName + "_" + queueName;
        return clientId;
    }
}
