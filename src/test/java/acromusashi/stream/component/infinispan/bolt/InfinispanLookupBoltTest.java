package acromusashi.stream.component.infinispan.bolt;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.component.infinispan.CacheHelper;
import acromusashi.stream.component.infinispan.SimpleCacheMapper;
import acromusashi.stream.component.infinispan.TupleCacheMapper;
import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * InfinispanLookupBoltのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class InfinispanLookupBoltTest
{
    /** テスト対象 */
    private InfinispanLookupBolt<String, String> target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector                      mockCollector;

    /** テスト用のStreamMessage */
    @Mock
    private StreamMessage                        mockMessage;

    /** テスト用のCache */
    @Mock
    private CacheHelper<String, String>          cacheHelper;

    /** テスト用のCache */
    @Mock
    private RemoteCache<String, String>          mockCache;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        TupleCacheMapper<String, String> mapper = new SimpleCacheMapper<>();
        this.target = new InfinispanLookupBolt<>("", "", mapper);

        Whitebox.setInternalState(this.target, "collector", this.mockCollector);
        Whitebox.setInternalState(this.target, "cacheHelper", this.cacheHelper);

        Mockito.when(this.cacheHelper.getCache()).thenReturn(this.mockCache);
    }

    /**
     * Keyの変換に失敗した場合、キャッシュから値の取得がされないことの確認を行う。
     *
     * @target {@link InfinispanLookupBolt#execute(Tuple)}
     * @test キャッシュから値の取得がされないこと
     *    condition::  Keyの変換に失敗
     *    result:: キャッシュから値の取得がされないこと
     */
    @Test
    public void testExecute_Key変換失敗()
    {
        // 準備
        Mockito.when(this.mockMessage.getField(FieldName.MESSAGE_KEY)).thenThrow(
                new IllegalArgumentException(FieldName.MESSAGE_KEY + " does not exist"));

        // 実施
        this.target.onExecute(this.mockMessage);

        // 検証
        Mockito.verify(this.mockCache, Mockito.never()).get(anyString());
    }

    /**
     * Keyの変換結果がnullだった場合、キャッシュから値の取得がされないことの確認を行う。
     *
     * @target {@link InfinispanLookupBolt#execute(Tuple)}
     * @test キャッシュから値の取得がされないこと
     *    condition::  Keyの変換結果がnullだった場合
     *    result:: キャッシュから値の取得がされないこと
     */
    @Test
    public void testExecute_KeyNull()
    {
        // 準備
        this.target = Mockito.spy(this.target);

        Mockito.when(this.mockMessage.getField(FieldName.MESSAGE_KEY)).thenReturn(null);

        // 実施
        this.target.onExecute(this.mockMessage);

        // 検証
        Mockito.verify(this.mockCache, Mockito.never()).get(anyString());
        Mockito.verify(this.target).onLookupAfter(this.mockMessage, null, null);
    }

    /**
     * Valueの取得に失敗した場合、取得後メソッドが呼ばれ、引数がValueのみnullであることを確認する
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test 取得後メソッドが呼ばれ、引数がValueのみnullであること
     *    condition::  Valueの取得に失敗した場合
     *    result:: 取得後メソッドが呼ばれ、引数がValueのみnullであること
     */
    @Test
    public void testExecute_Value取得失敗()
    {
        // 準備
        this.target = Mockito.spy(this.target);

        Mockito.when(this.mockMessage.getField(FieldName.MESSAGE_KEY)).thenReturn("MessageKey");
        Mockito.when(this.mockCache.get("MessageKey")).thenThrow(
                new HotRodClientException("Get failed."));

        // 実施
        this.target.onExecute(this.mockMessage);

        // 検証
        Mockito.verify(this.target).onLookupAfter(this.mockMessage, "MessageKey", null);
    }

    /**
     * Valueの取得結果がnullだった場合、取得後メソッドが呼ばれ、引数がValueのみnullであることを確認する
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test 取得後メソッドが呼ばれ、引数がValueのみnullであること
     *    condition::  Valueの取得結果がnullだった場合
     *    result:: 取得後メソッドが呼ばれ、引数がValueのみnullであること
     */
    @Test
    public void testExecute_Valuenull()
    {
        // 準備
        this.target = Mockito.spy(this.target);

        Mockito.when(this.mockMessage.getField(FieldName.MESSAGE_KEY)).thenReturn("MessageKey");
        Mockito.when(this.mockCache.get("MessageKey")).thenReturn(null);

        // 実施
        this.target.onExecute(this.mockMessage);

        // 検証
        Mockito.verify(this.target).onLookupAfter(this.mockMessage, "MessageKey", null);
    }

    /**
     * Valueの取得結果が存在した場合、取得後メソッドが呼ばれ、引数に取得した値が設定されていることを確認する
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test 取得後メソッドが呼ばれ、引数がValueのみnullであること
     *    condition::  Valueの取得結果がnullだった場合
     *    result:: 取得後メソッドが呼ばれ、引数がValueのみnullであること
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testExecute_Value値有()
    {
        // 準備
        this.target = Mockito.spy(this.target);

        Mockito.when(this.mockMessage.getField(FieldName.MESSAGE_KEY)).thenReturn("MessageKey");
        Mockito.when(this.mockCache.get("MessageKey")).thenReturn("MessageValue");

        // 実施
        this.target.onExecute(this.mockMessage);

        // 検証
        Mockito.verify(this.target).onLookupAfter(this.mockMessage, "MessageKey", "MessageValue");

        ArgumentCaptor<Tuple> anchorCaptor = ArgumentCaptor.forClass(Tuple.class);
        ArgumentCaptor<List> tupleCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(anchorCaptor.capture(), tupleCaptor.capture());

        assertThat(anchorCaptor.getValue(), nullValue());
        List<Object> argList = tupleCaptor.getValue();

        assertThat((String) argList.get(0), is("MessageKey"));
        assertThat(argList.get(1), instanceOf(StreamMessage.class));
        StreamMessage sendMessage = (StreamMessage) argList.get(1);
        assertThat(sendMessage.getField(FieldName.MESSAGE_VALUE).toString(), is("MessageValue"));
    }
}
