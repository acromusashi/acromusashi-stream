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

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.Maps;

/**
 * Common message header used in AcroMUSASHI Stream
 *
 * @author tsukano
 */
public class StreamMessageHeader implements Serializable
{
    /** serialVersionUID */
    private static final long   serialVersionUID = -3290857759292105445L;

    /** Default Version */
    public static final String  DEFAULT_VERSION  = "1.0";

    /** Message Key */
    private String              messageKey       = "";

    /** Message Identifier */
    private String              messageId        = "";

    /** TimeStamp */
    private long                timestamp        = 0;

    /** Message Source Identifier */
    private String              source           = "";

    /** Message Type */
    private String              type             = "";

    /** Message Protocol Version */
    private String              version          = DEFAULT_VERSION;

    /** Additional Header */
    private Map<String, String> additionalHeader;

    /**
     * Constructs instance.
     */
    public StreamMessageHeader()
    {}

    /**
     * @return the messageKey
     */
    public String getMessageKey()
    {
        return this.messageKey;
    }

    /**
     * @param messageKey the messageKey to set
     */
    public void setMessageKey(String messageKey)
    {
        this.messageKey = messageKey;
    }

    /**
     * @return the messageId
     */
    public String getMessageId()
    {
        return this.messageId;
    }

    /**
     * @param messageId the messageId to set
     */
    public void setMessageId(String messageId)
    {
        this.messageId = messageId;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp()
    {
        return this.timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    /**
     * @return the source
     */
    public String getSource()
    {
        return this.source;
    }

    /**
     * @param source the source to set
     */
    public void setSource(String source)
    {
        this.source = source;
    }

    /**
     * @return the type
     */
    public String getType()
    {
        return this.type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type)
    {
        this.type = type;
    }

    /**
     * @return the version
     */
    public String getVersion()
    {
        return this.version;
    }

    /**
     * @param version the version to set
     */
    public void setVersion(String version)
    {
        this.version = version;
    }

    /**
     * @return the additionalHeader
     */
    public Map<String, String> getAdditionalHeader()
    {
        return this.additionalHeader;
    }

    /**
     * @param additionalHeader the additionalHeader to set
     */
    public void setAdditionalHeader(Map<String, String> additionalHeader)
    {
        this.additionalHeader = additionalHeader;
    }

    /**
     * Add value to additional header.
     *
     * @param key key
     * @param value value
     */
    public void addAdditionalHeader(String key, String value)
    {
        if (this.additionalHeader == null)
        {
            this.additionalHeader = Maps.newLinkedHashMap();
        }

        this.additionalHeader.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String result = ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        return result;
    }
}
