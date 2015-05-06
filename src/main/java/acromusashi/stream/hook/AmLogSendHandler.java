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

import javax.websocket.SendHandler;
import javax.websocket.SendResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm log send result handler.
 * 
 * @author kimura
 */
public class AmLogSendHandler implements SendHandler
{
    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(AmLogSendHandler.class);

    /**
     * Default constructor.
     */
    public AmLogSendHandler()
    {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void onResult(SendResult result)
    {
        if (!result.isOK())
        {
            logger.info("Web socket send failed. Skip send. : Reason={}",
                    result.getException().getLocalizedMessage());
        }
    }
}
