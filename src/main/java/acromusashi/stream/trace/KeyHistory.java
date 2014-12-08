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
package acromusashi.stream.trace;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;

/**
 * キー情報の履歴を保持するValueObjectクラス
 */
public class KeyHistory implements Serializable
{
    /** serialVersionUID */
    private static final long   serialVersionUID = -7488577569585048930L;

    /** キー情報を出力する際のフォーマット */
    private static final String TOSTRING_FORMAT  = "KeyHistory={0}";

    /** 蓄積キー情報履歴。同様の値を重複して保持しないためにSetとして定義。*/
    protected Set<String>       keyHistory       = new LinkedHashSet<>();

    /**
     * パラメータを指定せずにインスタンスを作成する。
     */
    public KeyHistory()
    {
        // Do nothing.
    }

    /**
     * キー情報履歴に指定されたキーを追加する。<br>
     * ただし、既に同様の値がキー情報履歴に保持されている場合は追加を行わない。
     *
     * @param messageKey キー情報
     */
    public void addKey(String messageKey)
    {
        this.keyHistory.add(messageKey);
    }

    /**
     * 対象KeyHistoryInfoのディープコピーを生成し、返す。
     *
     * @return 対象KeyHistoryInfoのディープコピー
     */
    public KeyHistory createDeepCopy()
    {
        KeyHistory result = (KeyHistory) SerializationUtils.clone(this);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String keyHistoryStr = this.keyHistory.toString();
        String result = MessageFormat.format(TOSTRING_FORMAT, keyHistoryStr);
        return result;
    }
}
