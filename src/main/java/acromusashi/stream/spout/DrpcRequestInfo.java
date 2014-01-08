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
package acromusashi.stream.spout;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * DRPCリクエスト情報を保持するエンティティクラス
 * 
 * @author kimura
 */
public class DrpcRequestInfo implements Serializable
{
    /** serialVersionUID */
    private static final long serialVersionUID = -4167257782415832535L;

    /** リクエストID */
    private String            requestId;

    /** 引数 */
    private String            funcArgs;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public DrpcRequestInfo()
    {}

    /**
     * @return the requestId
     */
    public String getRequestId()
    {
        return this.requestId;
    }

    /**
     * @param requestId the requestId to set
     */
    public void setRequestId(String requestId)
    {
        this.requestId = requestId;
    }

    /**
     * @return the funcArgs
     */
    public String getFuncArgs()
    {
        return this.funcArgs;
    }

    /**
     * @param funcArgs the funcArgs to set
     */
    public void setFuncArgs(String funcArgs)
    {
        this.funcArgs = funcArgs;
    }

    @Override
    public String toString()
    {
        String result = ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        return result;
    }
}
