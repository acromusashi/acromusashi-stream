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
package acromusashi.stream.component.elasticsearch.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import acromusashi.stream.entity.StreamMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * TupleにElasticSearchに投入するJSONが含まれるケースに使用するコンバータ
 *
 * @author kimura
 */
public class JsonConverter implements EsTupleConverter
{
    /** serialVersionUID */
    private static final long      serialVersionUID = -6547492926366285428L;

    /** Index Name */
    private String                 index;

    /** Type Name */
    private String                 type;

    /** Documentが格納されているTupleField */
    private String                 field;

    /** JSONマッピング用のマッパー */
    private transient ObjectMapper mapper;

    /**
     * Index、Type、fieldを指定してインスタンスを生成する。
     *
     * @param index Index Name
     * @param type Type Name
     * @param field TupleField
     */
    public JsonConverter(String index, String type, String field)
    {
        this.index = index;
        this.type = type;
        this.field = field;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String convertToIndex(StreamMessage message)
    {
        return this.index;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String convertToType(StreamMessage message)
    {
        return this.type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String convertToId(StreamMessage message)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public String convertToDocument(StreamMessage message)
    {
        if (this.mapper == null)
        {
            this.mapper = new ObjectMapper();
        }

        String jsonStr = message.getField(this.field).toString();
        Map<String, Object> jsonMap = null;

        try
        {
            jsonMap = this.mapper.readValue(jsonStr, HashMap.class);
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }

        // Kibana用のタイムスタンプフィールドを追加
        jsonMap.put("@timestamp", jsonMap.get("time"));
        // 数値データをNumeric型の変数として投入するよう変換
        convertToNumeric(jsonMap);
        String result = null;

        try
        {
            result = this.mapper.writeValueAsString(jsonMap);
        }
        catch (JsonProcessingException ex)
        {
            throw new RuntimeException(ex);
        }

        return result;
    }

    /**
     * 受信したログデータに以下のフィールドが存在した場合、数値に変換する。<br>
     * 変換対象のフィールドは以下の通り。<br>
     *
     *
     * @param jsonMap 受信したログデータ(JSON)
     */
    private void convertToNumeric(Map<String, Object> jsonMap)
    {
        if (jsonMap.containsKey("reqtime") == true)
        {
            Long reqtime = Long.parseLong(jsonMap.get("reqtime").toString());
            jsonMap.put("reqtime", reqtime);
        }
        else
        {
            jsonMap.put("reqtime", 0L);
        }

        if (jsonMap.containsKey("reqtime_microsec") == true)
        {
            Long reqtimeMicro = Long.parseLong(jsonMap.get("reqtime_microsec").toString());
            jsonMap.put("reqtime_microsec", reqtimeMicro);
        }
        else
        {
            jsonMap.put("reqtime_microsec", 0L);
        }

        if (jsonMap.containsKey("size") == true)
        {
            String sizeStr = jsonMap.get("size").toString();

            if ("-".equals(sizeStr))
            {
                jsonMap.put("size", 0L);
            }
            else
            {
                Long size = Long.parseLong(sizeStr);
                jsonMap.put("size", size);
            }
        }
        else
        {
            jsonMap.put("size", 0L);
        }
    }
}
