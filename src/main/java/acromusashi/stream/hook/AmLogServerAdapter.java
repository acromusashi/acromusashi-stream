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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.DeploymentException;
import javax.websocket.SendHandler;
import javax.websocket.Session;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.elasticsearch.common.collect.Maps;
import org.glassfish.tyrus.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.hooks.info.EmitInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Storm Topology's Log Server adapter.
 * 
 * @author kimura
 */
public class AmLogServerAdapter
{
    /** Logger */
    private static final Logger       logger      = LoggerFactory.getLogger(AmLogServerAdapter.class);

    /** Adapter insance */
    private static AmLogServerAdapter instance;

    /** initialize flag */
    private boolean                   initialized = false;

    /** Object Mapper */
    private ObjectMapper              mapper;

    /** WebSocket server */
    private Server                    server;

    /** Websocket sessions */
    private Map<String, Session>      sessions;

    /** Log send handler. */
    private SendHandler               handler;

    /**
     * Default constructor
     */
    private AmLogServerAdapter()
    {
        this.mapper = new ObjectMapper();
        this.sessions = new ConcurrentHashMap<>();
        this.handler = new AmLogSendHandler();
    }

    /**
     * Get instance.
     * 
     * @return AmLogServerAdapter instance
     */
    public static synchronized AmLogServerAdapter getInstance()
    {
        if (instance == null)
        {
            instance = new AmLogServerAdapter();
        }

        return instance;
    }

    /**
     * Initialize WebSocket server.
     * 
     * @param serverPort serverPort
     */
    public synchronized void init(int serverPort)
    {
        if (this.initialized)
        {
            return;
        }

        String hostname = null;

        try
        {
            hostname = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException ex)
        {
            logger.warn("Get host failed. Use localhost.", ex);
        }

        logger.info("Websocket server initialize. : HostName={}, Port={}, Path={}, Object={}",
                hostname, serverPort, "/", this.toString());
        this.server = new Server(hostname, serverPort, "/", null, AmLogServerEndPoint.class);
        try
        {
            this.server.start();
        }
        catch (DeploymentException ex)
        {
            logger.warn("Websocket server initialize failed. Skip initialize.", ex);
        }

        this.initialized = true;
    }

    /**
     * 
     */
    public synchronized void finish()
    {
        if (this.server != null)
        {
            this.server.stop();
            this.server = null;
        }
    }

    /**
     * Websocket connection open.
     * 
     * @param session session
     */
    public void onOpen(Session session)
    {
        this.sessions.put(session.getId(), session);
    }

    /**
     * Websocket connection close.
     * 
     * @param session session
     */
    public void onClose(Session session)
    {
        this.sessions.remove(session.getId());
    }

    /**
     * Log emit info.
     * 
     * @param componentInfo component info
     * @param info emit info
     */
    public void emit(ComponentInfo componentInfo, EmitInfo info)
    {
        if (this.sessions.size() == 0)
        {
            return;
        }

        Map<String, Object> objectMap = Maps.newHashMap();
        objectMap.put("component", componentInfo);
        objectMap.put("emitinfo", info);

        String logInfo;
        try
        {
            logInfo = this.mapper.writeValueAsString(objectMap);
        }
        catch (JsonProcessingException ex)
        {
            String msgFormat = "Event convert failed. Skip log output. : ComponentInfo={0}, EmitInfo={1}";
            String message = MessageFormat.format(msgFormat, componentInfo,
                    ToStringBuilder.reflectionToString(info, ToStringStyle.SHORT_PREFIX_STYLE));
            logger.warn(message, ex);
            return;
        }

        for (Entry<String, Session> entry : this.sessions.entrySet())
        {
            entry.getValue().getAsyncRemote().sendText(logInfo, this.handler);
        }
    }
}
