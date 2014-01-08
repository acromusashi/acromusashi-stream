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
package acromusashi.stream.helper;

import java.io.Serializable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * SpringContextを扱うためのヘルパークラス
 * 
 * @author kimura
 */
public class SpringContextHelper implements Serializable
{
    /** serialVersionUID */
    private static final long          serialVersionUID = -3865604065504036049L;

    /** ApplicationContext。ユニットテスト用にtransientとしていない。 */
    private AbstractApplicationContext appContext;

    /** ApplicationContextPath */
    private String                     appContextPath;

    /**
     * コンテキストパスを渡してインスタンスを生成する。
     * 
     * @param appContextPath コンテキストパス
     */
    public SpringContextHelper(String appContextPath)
    {
        this.appContextPath = appContextPath;
    }

    /**
     * アプリケーションコンテキストから指定した型を持つコンポーネントを取得する。
     * 
     * @param componentType コンポーネント型
     * 
     * @param <T> 取得するコンポーネントの総称型パラメータ
     * @return 取得コンポーネント
     */
    public <T> T getComponent(Class<T> componentType)
    {
        BeanFactory factory = getBeanFactory();
        T component = factory.getBean(componentType);
        return component;
    }

    /**
     * アプリケーションコンテキストから指定した名称、型を持つコンポーネントを取得する。
     * 
     * @param componentName コンポーネント名称
     * @param componentType コンポーネント型
     * 
     * @param <T> 取得するコンポーネントの総称型パラメータ
     * @return 取得コンポーネント
     */
    public <T> T getComponent(String componentName, Class<T> componentType)
    {
        BeanFactory factory = getBeanFactory();
        T component = factory.getBean(componentName, componentType);
        return component;
    }

    /**
     * BeanFactoryを取得する。<br>
     * 個別のContextがあらかじめ設定されていればContextを用いてBeanFactoryを生成する。<br>
     * 設定されていない場合はデフォルトのContextを生成して用いる。
     * 
     * @return BeanFactory
     */
    protected BeanFactory getBeanFactory()
    {
        if (this.appContext == null)
        {
            this.appContext = new GenericXmlApplicationContext(this.appContextPath);
        }

        return this.appContext.getBeanFactory();
    }

    /**
     * @return the appContext
     */
    public AbstractApplicationContext getAppContext()
    {
        return this.appContext;
    }

    /**
     * @param appContext the appContext to set
     */
    public void setAppContext(AbstractApplicationContext appContext)
    {
        this.appContext = appContext;
    }
}
