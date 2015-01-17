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
package acromusashi.stream.component.elasticsearch.bolt;

import java.io.Serializable;

import acromusashi.stream.entity.StreamMessage;

/**
 * Storm TupleをElastic Searchに投入するIndex Requestに変換するコンバータインタフェース
 *
 * @author kimura
 */
public interface EsTupleConverter extends Serializable
{
    /**
     * Storm TupleからElastic SearchにIndex Requestを投入する際のIndex Nameを生成する
     *
     * @param message received message
     * @return Index Name
     */
    String convertToIndex(StreamMessage message);

    /**
     * Storm TupleからElastic SearchにIndex Requestを投入する際のType Nameを生成する
     *
     * @param message received message
     * @return Type Name
     */
    String convertToType(StreamMessage message);

    /**
     * Storm TupleからElastic SearchにIndex Requestを投入する際のDocument IDを生成する
     *
     * @param message received message
     * @return Document ID
     */
    String convertToId(StreamMessage message);

    /**
     * Storm TupleからElastic SearchにIndex Requestを投入する際のDocumentを生成する
     *
     * @param message received message
     * @return Document
     */
    String convertToDocument(StreamMessage message);
}
