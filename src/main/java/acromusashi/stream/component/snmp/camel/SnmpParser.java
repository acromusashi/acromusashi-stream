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
package acromusashi.stream.component.snmp.camel;

import java.util.Date;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.snmp4j.smi.UdpAddress;

/**
 * Exchangeオブジェクトをパースし、メッセージボディに詰め込む処理
 * 
 * @author tsukano
 */
public class SnmpParser
{
    /**
     * デフォルトコンストラクタ
     */
    public SnmpParser()
    {}

    /**
     * SNMPを含むExchangeをパースする
     * 
     * @param exchange SNMPを含むExchange
     */
    public void parseSNMP(Exchange exchange)
    {
        Message inMsg = exchange.getIn();
        Map<String, Object> inHeaders = inMsg.getHeaders();
        exchange.getOut().setHeaders(inHeaders);

        StringBuilder body = new StringBuilder();
        body.append("<message>");

        body.append("<header>");

        body.append("<sender>");
        body.append(((UdpAddress) inHeaders.get("peerAddress")).getInetAddress().getHostAddress());
        body.append("</sender>");

        body.append("<type>");
        body.append("snmp");
        body.append("</type>");

        body.append("<timestamp>");
        body.append(new Date().getTime());
        body.append("</timestamp>");

        body.append("<version>");
        body.append("1.0");
        body.append("</version>");

        body.append("</header>");

        body.append("<body>");
        body.append(inMsg.getBody(String.class));
        body.append("</body>");

        body.append("</message>");

        exchange.getOut().setBody(body.toString());
    }
}
