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
package acromusashi.stream.component.snmp.converter;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import acromusashi.stream.converter.AbstractMessageConverter;
import acromusashi.stream.entity.StreamMessageHeader;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.ConvertFailException;

/**
 * SNMPTrap(JSON形式)をMessageに変換するコンバータ。
 * 
 * @author kimura
 */
public class SnmpConverter extends AbstractMessageConverter
{
    /** serialVersionUID */
    private static final long  serialVersionUID = -7731469104391119319L;

    /** snmpTrapOIDを表すOID */
    public static final String SNMP_TRAP_OID    = "1.3.6.1.6.3.1.1.4.1.0";

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public SnmpConverter()
    {}

    @Override
    public String getType()
    {
        return "snmp";
    }

    @Override
    public StreamMessageHeader createHeader(Object input)
    {
        JSONObject json = JSONObject.fromObject(input);
        JSONObject jsonHeader = json.getJSONObject("header");

        StreamMessageHeader header = new StreamMessageHeader();
        header.setTimestamp(jsonHeader.getLong("timestamp"));
        header.setSource(jsonHeader.getString("sender"));
        header.setType(getType());
        header.addAdditionalHeader("SNMPVersion", jsonHeader.getString("version"));

        return header;
    }

    @Override
    public Object createBody(Object input)
    {
        JSONObject json = JSONObject.fromObject(input);
        JSONObject jsonBody = json.getJSONObject("body");

        // Bodyを生成
        // Listの最初の要素にsnmpTrapOIDを詰める。
        // 2個目の要素に"oid1=value1;oid2=value2"という形式で詰める。
        JSONArray jsonVarBinds = jsonBody.getJSONArray("snmp");
        String trapOid = null;
        StringBuilder varBinds = new StringBuilder();
        for (int index = 0; index < jsonVarBinds.size(); index++)
        {
            JSONObject jsonVarBind = jsonVarBinds.getJSONObject(index);
            String oid = jsonVarBind.getString("oid");
            String value = jsonVarBind.getString("value");

            if (SNMP_TRAP_OID.endsWith(oid))
            {
                trapOid = value;
            }
            else
            {
                if (varBinds.length() > 0)
                {
                    varBinds.append(';');
                }
                varBinds.append(oid);
                varBinds.append('=');
                varBinds.append(value);
            }
        }
        List<Object> list = new ArrayList<Object>();
        list.add(trapOid);
        list.add(varBinds.toString());

        return list;
    }

    /**
     * メッセージをKey-ValueのMapに変換する。<br>
     * 
     * @param message メッセージ
     * @return 変換したKey-ValueのMap
     * @throws ConvertFailException 変換失敗時
     */
    @Override
    @SuppressWarnings("rawtypes")
    public Map<String, Object> toMap(StreamMessage message) throws ConvertFailException
    {
        Map<String, Object> result = new LinkedHashMap<String, Object>();

        // Messageヘッダのうち、DBに格納する値を取得する
        result.put("messageId", message.getHeader().getMessageId());
        result.put("timestamp", new Timestamp(message.getHeader().getTimestamp()));

        result.put("source", message.getHeader().getSource());

        List<String> bodyStrList = new ArrayList<String>();
        // BodyがIterableの場合はStringリスト形式としてDB格納値を取得
        // Iterableでない場合はtoStringの結果をそのまま詰める。
        if (message.getBody() instanceof Iterable)
        {
            Iterator list = ((Iterable) message.getBody()).iterator();
            while (list.hasNext())
            {
                bodyStrList.add(list.next().toString());
            }
        }
        else
        {
            bodyStrList.add(message.getBody().toString());
        }

        result.put("body", bodyStrList);

        return result;
    }
}
