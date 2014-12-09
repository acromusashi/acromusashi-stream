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

/**
 * StormTopologyの実行状態を示す列挙型
 *
 * @author kimura
 */
public enum TopologyExecutionStatus
{
    /** メッセージ処理中 */
    EXECUTING("EXECUTING"),

    /** メッセージ処理停止中 */
    STOP("STOP"),

    /** Topology未存在 */
    NOT_ALIVED("NOT_ALIVED");

    /** パラメータのキー名 */
    private String key;

    /**
     * キー名を指定してインスタンスを生成する。
     *
     * @param key パラメータのキー名
     */
    private TopologyExecutionStatus(String key)
    {
        this.key = key;
    }

    /**
     * @return key パラメータのキー名
     */
    public String getKey()
    {
        return this.key;
    }
}
