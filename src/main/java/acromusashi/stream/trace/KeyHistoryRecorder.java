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

import java.util.List;

/**
 * Stormのメッセージにキー情報履歴を保存するユーティリティクラス
 *
 * @author kimura
 */
public class KeyHistoryRecorder
{
    /**
     * 外部からのインスタンス化を防止するデフォルトコンストラクタ。
     */
    private KeyHistoryRecorder()
    {
        // Do nothing.
    }

    /**
     * ベースとなるメッセージにキー履歴情報を追加する。
     *
     * @param baseMessage ベースメッセージ
     */
    public static final void recordKeyHistory(List<Object> baseMessage)
    {
        recordKeyHistory(baseMessage, null);
    }

    /**
     * ベースとなるメッセージにキー履歴情報を追加する。
     *
     * @param baseMessage ベースメッセージ
     * @param messageKey メッセージキー
     */
    public static final void recordKeyHistory(List<Object> baseMessage, Object messageKey)
    {
        KeyHistory history = new KeyHistory();

        if (messageKey != null)
        {
            history.addKey(messageKey.toString());
        }

        baseMessage.add(0, history);
    }
}
