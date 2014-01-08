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
package acromusashi.stream.camel.debug;

import java.io.IOException;

import org.apache.camel.Exchange;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 受信したExchange内容をダンプ出力するデバッグ用クラス
 * 
 * @author tsukano
 */
public class ExchangeDumper
{
    /** logger */
    private static final Logger logger = LoggerFactory.getLogger(ExchangeDumper.class);

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public ExchangeDumper()
    {}

    /**
     * Exchangeのdebug用メソッド。
     * 
     * @param exchange debug対象のExchange
     * @throws IOException 出力失敗時
     */
    public void dump(Exchange exchange) throws IOException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(ToStringBuilder.reflectionToString(exchange.getIn().getBody(),
                    ToStringStyle.SHORT_PREFIX_STYLE));
        }
    }
}
