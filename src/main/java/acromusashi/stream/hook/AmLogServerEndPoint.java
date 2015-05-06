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

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm log websocketserver endpoint.
 * 
 * @author kimura
 */
@ServerEndpoint(value = "/log")
public class AmLogServerEndPoint
{
    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(AmLogServerEndPoint.class);

    /**
     * Default constructor
     */
    public AmLogServerEndPoint()
    {}

    /**
     * Websocket connection open.
     * 
     * @param session session
     */
    @OnOpen
    public void onOpen(Session session)
    {
        logger.info("WebSocket connected. : SessionId={}", session.getId());
    }

    /**
     * Websocket connection close.
     * 
     * @param session session 
     * @param closeReason closeReason
     */
    @OnClose
    public void onClose(Session session, CloseReason closeReason)
    {
        logger.info("WebSocket closed. : SessionId={}, Reason={}", session.getId(),
                closeReason.toString());
    }
}
