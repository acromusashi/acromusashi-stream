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
package acromusashi.stream.entity;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Common message used in AcroMUSASHI Stream
 *
 * @author tsukano
 */
public class StreamMessage implements Serializable
{
    /** serialVersionUID */
    private static final long   serialVersionUID = -7553630425199269093L;

    /** Message Header */
    private StreamMessageHeader header;

    /** Message Body */
    private Object              body;

    /**
     * Constructs instance.
     */
    public StreamMessage()
    {}

    /**
     * Constructs instance with Header and body.
     *
     * @param header Message Header
     * @param body Message Body
     */
    public StreamMessage(StreamMessageHeader header, Object body)
    {
        this.header = header;
        this.body = body;
    }

    /**
     * @return the header
     */
    public StreamMessageHeader getHeader()
    {
        return this.header;
    }

    /**
     * @param header the header to set
     */
    public void setHeader(StreamMessageHeader header)
    {
        this.header = header;
    }

    /**
     * @return the body
     */
    public Object getBody()
    {
        return this.body;
    }

    /**
     * @param body the body to set
     */
    public void setBody(Object body)
    {
        this.body = body;
    }

    /**
     * Add field to message body.
     *
     * @param key key
     * @param value value
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void addField(String key, Object value)
    {
        if (this.body == null || Map.class.isAssignableFrom(this.body.getClass()) == false)
        {
            this.body = Maps.newLinkedHashMap();
        }

        ((Map) this.body).put(key, value);
    }

    /**
     * Get field from message body.
     *
     * @param key key
     * @return field mapped key
     */
    @SuppressWarnings("rawtypes")
    public Object getField(String key)
    {
        if (this.body == null || Map.class.isAssignableFrom(this.body.getClass()) == false)
        {
            return null;
        }

        return ((Map) this.body).get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String result = this.header.toString() + "," + this.body.toString();
        return result;
    }
}
