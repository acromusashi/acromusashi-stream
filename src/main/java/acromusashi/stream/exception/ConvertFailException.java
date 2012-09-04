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
 * メッセージ変換時に発生する共通例外クラス
 * 
 * @author kimura
 */
public class ConvertFailException extends Exception
{
    /** serialVersionUID */
    private static final long serialVersionUID = -3457655487597730586L;

    /**
     * デフォルトコンストラクタ
     */
    public ConvertFailException()
    {
        super();
    }

    /**
     * コンストラクタ
     * 
     * @param message メッセージ
     */
    public ConvertFailException(String message)
    {
        super(message);
    }

    /**
     * コンストラクタ
     * 
     * @param message メッセージ
     * @param cause 発生原因例外
     */
    public ConvertFailException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * コンストラクタ
     * 
     * @param cause 発生原因例外
     */
    public ConvertFailException(Throwable cause)
    {
        super(cause);
    }
}
