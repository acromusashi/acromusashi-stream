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

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout.BatchCoordinator;

/**
 * DRPCリクエストを受信してTridentの処理を開始するSpout用のCoodinatorクラス
 * 
 * @author kimura
 */
public class DrpcTridentCoodinator implements BatchCoordinator<Object>
{
    /** ロガー */
    private static Logger logger = LoggerFactory.getLogger(DrpcTridentCoodinator.class);

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public DrpcTridentCoodinator()
    {}

    /**
     * {@inheritDoc}
     */
    @Override
    public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata)
    {
        if (logger.isDebugEnabled() == true)
        {
            String logFormat = "initializeTransaction : txid={0}, prevMetadata={1}, currMetadata={2}";
            logger.debug(MessageFormat.format(logFormat, txid, prevMetadata, currMetadata));
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void success(long txid)
    {
        if (logger.isDebugEnabled() == true)
        {
            logger.debug("success txid=" + txid);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReady(long txid)
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        // Do nothing.
    }
}
