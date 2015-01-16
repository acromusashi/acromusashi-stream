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

/**
 * AcroMUSASHI Stream内で流通する汎用メッセージ。
 *
 * @author tsukano
 */
public class StreamMessage implements Serializable
{
    /** serialVersionUID */
    private static final long serialVersionUID = -7553630425199269093L;

    /** Message Header */
    private StreamMessageHeader            header;

    /** Message Body */
    private Object            body;

    /**
     * Class constructor.
     */
    public StreamMessage()
    {}

    /**
     * ヘッダとボディを指定してインスタンスを生成する。
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
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String result = this.header.toString() + "," + this.body.toString();
        return result;
    }
}
