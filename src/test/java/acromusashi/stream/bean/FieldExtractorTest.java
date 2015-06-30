package acromusashi.stream.bean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.apache.storm.guava.collect.Maps;
import org.junit.Test;

import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.entity.StreamMessageHeader;

/**
 * FieldExtractorのテストクラス
 * 
 * @author kimura
 */
public class FieldExtractorTest
{

    /**
     * 1文字のDelimeterで正しくキーの抽出が行われることを確認する。
     *
     * @target {@link FieldExtractor#extractKeyHead(String, String)}
     * @test キー抽出が行われること。
     *    condition:: 1文字のDelimeterでキー抽出を行う。
     *    result:: キー抽出が行われること。
     */
    @Test
    public void extract_KeyExtract_1CharDelim()
    {
        // prepare
        String baseKey = "data_text_inner";
        String expectedHead = "data";
        String expectedTail = "text_inner";

        // execute
        String actualHead = FieldExtractor.extractKeyHead(baseKey, "_");
        String actualTail = FieldExtractor.extractKeyTail(baseKey, "_");

        // assert
        assertThat(actualHead, is(expectedHead));
        assertThat(actualTail, is(expectedTail));
    }

    /**
     * 2文字のDelimeterで正しくキーの抽出が行われることを確認する。
     *
     * @target {@link FieldExtractor#extractKeyHead(String, String)}
     * @test キー抽出が行われること。
     *    condition:: 2文字のDelimeterでキー抽出を行う。
     *    result:: キー抽出が行われること。
     */
    @Test
    public void extract_KeyExtract_2CharDelim()
    {
        // prepare
        String baseKey = "data__text__inner";
        String expectedHead = "data";
        String expectedTail = "text__inner";

        // execute
        String actualHead = FieldExtractor.extractKeyHead(baseKey, "__");
        String actualTail = FieldExtractor.extractKeyTail(baseKey, "__");

        // assert
        assertThat(actualHead, is(expectedHead));
        assertThat(actualTail, is(expectedTail));
    }

    /**
     * 3文字のDelimeterで正しくキーの抽出が行われることを確認する。
     *
     * @target {@link FieldExtractor#extractKeyHead(String, String)}
     * @test キー抽出が行われること。
     *    condition:: 3文字のDelimeterでキー抽出を行う。
     *    result:: キー抽出が行われること。
     */
    @Test
    public void extract_KeyExtract_3CharDelim()
    {
        // prepare
        String baseKey = "data___text___inner";
        String expectedHead = "data";
        String expectedTail = "text___inner";

        // execute
        String actualHead = FieldExtractor.extractKeyHead(baseKey, "___");
        String actualTail = FieldExtractor.extractKeyTail(baseKey, "___");

        // assert
        assertThat(actualHead, is(expectedHead));
        assertThat(actualTail, is(expectedTail));
    }

    /**
     * 3文字のDelimeterで正しくキーの抽出が行われることを確認する。
     *
     * @target {@link FieldExtractor#extractKeyHead(String, String)}
     * @test キー抽出が行われること。
     *    condition:: 3文字のDelimeterでキー抽出を行う。
     *    result:: キー抽出が行われること。
     */
    @Test
    public void extract_KeyExtract_HeadOnly_3CharDelim()
    {
        // prepare
        String baseKey = "data__text__inner";
        String expectedHead = "data__text__inner";

        // execute
        String actualHead = FieldExtractor.extractKeyHead(baseKey, "___");
        String actualTail = FieldExtractor.extractKeyTail(baseKey, "___");

        // assert
        assertThat(actualHead, is(expectedHead));
        assertNull(actualTail);
    }

    /**
     * MapのParameterを指定して値を抽出できることを確認する。
     *
     * @target {@link FieldExtractor#extract(Object, String, String)}
     * @test 値を抽出できること。
     *    condition:: 2文字のDelimeterでキー抽出を行う。
     *    result:: 値を抽出できること。
     */
    @Test
    public void extract_MapParameter()
    {
        // prepare
        Map<String, Object> src = Maps.newLinkedHashMap();
        src.put("test_field1", "Test1");
        src.put("testField2", "Test2");
        Map<String, Object> innerMap = Maps.newLinkedHashMap();
        innerMap.put("inner_field1", "Inner1");
        src.put("test_field3", innerMap);

        // execute
        Object actual = FieldExtractor.extract(src, "test_field3__inner_field1", "__");

        // assert
        assertThat(actual.toString(), is("Inner1"));
    }
    
    /**
     * EntityのParameterを指定して値を抽出できることを確認する。
     *
     * @target {@link FieldExtractor#extract(Object, String, String)}
     * @test 値を抽出できること。
     *    condition:: 2文字のDelimeterでキー抽出を行う。
     *    result:: 値を抽出できること。
     */
    @Test
    public void extract_EntityParameter()
    {
        // prepare
        StreamMessage message = new StreamMessage();
        StreamMessageHeader header = new StreamMessageHeader();
        header.setMessageKey("messageKey");
        message.setHeader(header);

        // execute
        Object actualMessageKey = FieldExtractor.extract(message, "header__messageKey", "__");
        Object actualHistory = FieldExtractor.extract(message, "header__history", "__");
        Object nullArg = FieldExtractor.extract(null, "header__history", "__");

        // assert
        assertThat(actualMessageKey.toString(), is("messageKey"));
        assertNull(actualHistory);
        assertNull(nullArg);
    }
}
