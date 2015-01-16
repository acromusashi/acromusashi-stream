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
package acromusashi.stream.converter;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtilsBean;

import acromusashi.stream.entity.StreamMessageHeader;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.ConvertFailException;

/**
 * メッセージコンバータ基底クラス<br/>
 * 本クラスを継承して個別のコンバータクラスを作成すること。
 * 
 * @author kimura
 */
public abstract class AbstractMessageConverter implements Serializable
{
    /** serialVersionUID　*/
    private static final long           serialVersionUID = 7493668537764586461L;

    /** メッセージ変換用のフィールド。toMapメソッドにてクラス情報をキャッシュするために用いる。 */
    private transient PropertyUtilsBean propUtilBean;

    /**
     * 変換対象メッセージの型を取得する。
     * 
     * @return 変換対象メッセージの型
     */
    public abstract String getType();

    /**
      * 受信Objectを基に共通メッセージのHeaderを生成する。<br/>
      * デフォルト動作としてはgetType()の結果をTypeに詰める。
      * Type以外のパラメータについては何もしない。
      * 
      * @param input 受信Object
      * @return 共通メッセージHeader
      * @throws ConvertFailException 変換失敗時
      */
    public StreamMessageHeader createHeader(Object input) throws ConvertFailException
    {
        StreamMessageHeader header = new StreamMessageHeader();
        header.setType(getType());

        return header;
    }

    /**
     * 受信Objectを基に共通メッセージのBodyを生成する。<br/>
     * デフォルト動作はinput.toString()の結果をBodyとして返す。
     * 
     * @param input 受信Object
     * @return 共通メッセージBody
     * @throws ConvertFailException 変換失敗時
     */
    public Object createBody(Object input) throws ConvertFailException
    {
        // デフォルト動作として受信ObjectのtoStringの結果を使用
        return input.toString();
    }

    /**
     * メッセージをKey-ValueのMapに変換する。<br/>
     * 
     * @param message メッセージ
     * @return 変換したKey-ValueのMap
     * @throws ConvertFailException 変換失敗時
     */
    public Map<String, Object> toMap(StreamMessage message) throws ConvertFailException
    {
        if (this.propUtilBean == null)
        {
            this.propUtilBean = new PropertyUtilsBean();
        }

        // デフォルト動作としてソースコード上の記述順に
        // Key(フィールド名)、Value（フィールド値)を取得する。
        Map<String, Object> result = new LinkedHashMap<String, Object>();

        try
        {
            PropertyDescriptor[] descriptors = this.propUtilBean.getPropertyDescriptors(message);
            for (int i = 0; i < descriptors.length; i++)
            {
                String name = descriptors[i].getName();
                if (descriptors[i].getReadMethod() != null)
                {
                    result.put(name, this.propUtilBean.getProperty(message, name));
                }
            }
        }
        catch (Exception ex)
        {
            // AbstractMessageConverterのデフォルト動作においては
            // IllegalAccessException、InvocationTargetException、NoSuchMethodExceptionの
            // 3種類の例外が発生するが、継承クラスのインタフェースへの影響を抑えるため、
            // 例外発生時は一律でConvertFailExceptionクラスでラッピングして投げるようにする。
            throw new ConvertFailException(ex);
        }

        return result;
    }
}
