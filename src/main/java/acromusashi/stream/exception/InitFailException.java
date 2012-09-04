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
package acromusashi.stream.exception;

/**
 * Spout/Boltの初期化失敗時に投げる実行時例外
 * 
 * @author kimura
 */
public class InitFailException extends RuntimeException
{
    /** serialVersionUID */
    private static final long serialVersionUID = 70574633960070251L;

    /**
     * デフォルトコンストラクタ
     */
    public InitFailException()
    {
        super();
    }

    /**
     * コンストラクタ
     * 
     * @param message メッセージ
     */
    public InitFailException(String message)
    {
        super(message);
    }

    /**
     * コンストラクタ
     * 
     * @param message メッセージ
     * @param cause 発生原因例外
     */
    public InitFailException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * コンストラクタ
     * 
     * @param cause 発生原因例外
     */
    public InitFailException(Throwable cause)
    {
        super(cause);
    }
}
