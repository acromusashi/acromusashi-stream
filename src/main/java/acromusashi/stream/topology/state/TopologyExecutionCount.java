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
package acromusashi.stream.topology.state;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Topologyの処理件数を保持するValueObjectクラス
 *
 * @author kimura
 */
public class TopologyExecutionCount
{
    /** TupleEmit数 */
    private long emitted;

    /** Tuple送信数 */
    private long transferred;

    /** Spoutに対するack数 */
    private long acked;

    /** Spoutに対するfail数 */
    private long failed;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public TopologyExecutionCount()
    {
        // Do nothing.
    }

    /**
     * @return the emitted
     */
    public long getEmitted()
    {
        return this.emitted;
    }

    /**
     * @param emitted the emitted to set
     */
    public void setEmitted(long emitted)
    {
        this.emitted = emitted;
    }

    /**
     * @return the transferred
     */
    public long getTransferred()
    {
        return this.transferred;
    }

    /**
     * @param transferred the transferred to set
     */
    public void setTransferred(long transferred)
    {
        this.transferred = transferred;
    }

    /**
     * @return the acked
     */
    public long getAcked()
    {
        return this.acked;
    }

    /**
     * @param acked the acked to set
     */
    public void setAcked(long acked)
    {
        this.acked = acked;
    }

    /**
     * @return the failed
     */
    public long getFailed()
    {
        return this.failed;
    }

    /**
     * @param failed the failed to set
     */
    public void setFailed(long failed)
    {
        this.failed = failed;
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
