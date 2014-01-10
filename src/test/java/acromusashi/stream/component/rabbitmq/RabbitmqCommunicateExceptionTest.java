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
package acromusashi.stream.component.rabbitmq;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * RabbitmqCommunicateExceptionの生成テストケース
 * 
 * @author kimura
 */
public class RabbitmqCommunicateExceptionTest
{

    /**
     * 引数を指定しないケースの生成結果を確認
     * 
     * @target {@link RabbitmqCommunicateException}
     * @test 引数なしの生成結果を確認
     *    condition:: 引数なしでクラスを生成
     *    result:: 結果が完全修飾クラス名となること
     */
    @Test
    public void testConstructor_引数なし()
    {
        // 実施
        RabbitmqCommunicateException actual = new RabbitmqCommunicateException();

        // 検証
        assertThat(
                actual.toString(),
                is("acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException"));
    }

    /**
     * メッセージを指定するケースの生成結果を確認
     * 
     * @target {@link RabbitmqCommunicateException}
     * @test メッセージ指定の生成結果を確認
     *    condition:: メッセージ指定でクラスを生成
     *    result:: 結果が完全修飾クラス名 + メッセージとなること
     */
    @Test
    public void testConstructor_メッセージ指定()
    {
        // 実施
        RabbitmqCommunicateException actual = new RabbitmqCommunicateException("TestMessage");

        // 検証
        assertThat(
                actual.toString(),
                is("acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException: TestMessage"));
    }

    /**
     * 発生原因例外を指定するケースの生成結果を確認
     * 
     * @target {@link RabbitmqCommunicateException}
     * @test 発生原因例外の生成結果を確認
     *    condition:: 発生原因例外でクラスを生成
     *    result:: 結果が完全修飾クラス名 + 発生原因例外の完全修飾クラス名となること
     */
    @Test
    public void testConstructor_原因指定()
    {
        // 実施
        RabbitmqCommunicateException actual = new RabbitmqCommunicateException(new Exception());

        // 検証
        assertThat(
                actual.toString(),
                is("acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException: java.lang.Exception"));
    }

    /**
     * メッセージ、発生原因例外を指定するケースの生成結果を確認
     * 
     * @target {@link RabbitmqCommunicateException}
     * @test メッセージ、発生原因例外を指定の生成結果を確認
     *    condition:: メッセージ、発生原因例外を指定でクラスを生成
     *    result:: 結果が完全修飾クラス名 + メッセージとなること
     */
    @Test
    public void testConstructor_メッセージ例外指定()
    {
        // 実施
        RabbitmqCommunicateException actual = new RabbitmqCommunicateException("TestMessage",
                new Exception());

        // 検証
        assertThat(
                actual.toString(),
                is("acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException: TestMessage"));
    }
}
