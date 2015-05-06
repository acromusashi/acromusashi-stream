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
package acromusashi.stream.hook;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Storm log send result handler.
 * 
 * @author kimura
 */
public class ComponentInfo
{
    /** Component Id */
    private String componentId;

    /** Task id */
    private int    taskId;

    /**
     * Constructor with params.
     * 
     * @param componentId Component Id
     * @param taskId Task id
     */
    public ComponentInfo(String componentId, int taskId)
    {
        this.componentId = componentId;
        this.taskId = taskId;
    }

    /**
     * @return the componentId
     */
    public String getComponentId()
    {
        return this.componentId;
    }

    /**
     * @param componentId the componentId to set
     */
    public void setComponentId(String componentId)
    {
        this.componentId = componentId;
    }

    /**
     * @return the taskId
     */
    public int getTaskId()
    {
        return this.taskId;
    }

    /**
     * @param taskId the taskId to set
     */
    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
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
