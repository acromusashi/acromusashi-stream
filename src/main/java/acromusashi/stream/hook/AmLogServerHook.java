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

import java.util.Map;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;

/**
 * Storm Topology's Log Server hook.
 * 
 * @author kimura
 */
public class AmLogServerHook extends BaseTaskHook
{
    /** Default port adjust */
    private static final int DEFAULT_PORT_ADJUST = 10000;

    /** ComponentInfo */
    private ComponentInfo    componentInfo;

    /**
     * Default constructor
     */
    public AmLogServerHook()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context)
    {
        this.componentInfo = new ComponentInfo(context.getThisComponentId(),
                context.getThisTaskId());

        int workerPort = context.getThisWorkerPort();
        int portAdjust = DEFAULT_PORT_ADJUST;
        if (conf.containsKey("log.server.port.adjust"))
        {
            portAdjust = Integer.parseInt(conf.get("log.server.port.adjust").toString());
        }

        int serverPort = workerPort + portAdjust;
        AmLogServerAdapter.getInstance().init(serverPort);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void emit(EmitInfo info)
    {
        AmLogServerAdapter.getInstance().emit(this.componentInfo, info);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup()
    {
        AmLogServerAdapter.getInstance().finish();
    }
}
