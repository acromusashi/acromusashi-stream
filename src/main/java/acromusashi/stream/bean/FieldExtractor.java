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
package acromusashi.stream.bean;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanWrapperImpl;

/**
 * 対象からフィールドをEL式で抽出するユーティリティ
 * 
 * @author kimura
 */
public class FieldExtractor
{
    /**
     * Constructor
     */
    private FieldExtractor()
    {}

    /**
     * Extract field 
     * 
     * @param target is used to get object data.
     * @param key is used to get key value.
     * @return is used to return object data.
     */
    public static Object extract(Object target, String key)
    {
        String keyHead = getKeyHead(key); // the return value of keyHead 
        String keyTail = getKeyTail(key); // the return value of keyTail 

        Object innerObject = null;

        // checking if the "Object: target" is Map or not
        if (target instanceof Map)
        {
            Map<?, ?> targetMap = (Map<?, ?>) target;
            innerObject = targetMap.get(keyHead); // get the object inside "keyHead"
        }
        else
        {
            BeanWrapperImpl baseWapper = new BeanWrapperImpl(target); // this is the reflection for getting the field value.
            innerObject = baseWapper.getPropertyValue(keyHead); // get the value from the field if the "keyHead" is not Map
        }

        if (innerObject == null)
        {
            return innerObject;
        }

        if (StringUtils.isEmpty(keyTail) == true)
        {
            return innerObject;
        }
        else
        {
            return extract(innerObject, keyTail); // recursive method for calling self function again.
        }
    }

    /**
     * To get the value of keyHead.
     * 
     * @param key is used to get string.
     * @return the  value of keyHead .
     */
    public static String getKeyHead(String key)
    {
        int index = key.indexOf(".");
        if (index == -1)
        {
            return key;
        }

        String result = key.substring(0, index);
        return result;
    }

    /**
     * To get the value of keyTail.
     * 
     * @param key is used to get string.
     * @return the  value of keyTail .
     */
    public static String getKeyTail(String key)
    {
        int index = key.indexOf(".");
        if (index == -1)
        {
            return null;
        }
        String result = key.substring(index + 1);
        return result;
    }
}
