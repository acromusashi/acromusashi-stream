package acromusashi.stream.component.infinispan.bolt;

import static org.mockito.Matchers.anyString;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.component.infinispan.CacheHelper;
import acromusashi.stream.component.infinispan.SimpleCacheMapper;
import acromusashi.stream.component.infinispan.TupleCacheMapper;
import acromusashi.stream.constants.FieldName;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * InfinispanStoreBoltのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class InfinispanStoreBoltTest
{
    /** テスト対象 */
    private InfinispanStoreBolt<String, String> target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector                     mockCollector;

    /** テスト用のTuple */
    @Mock
    private Tuple                               mockTuple;

    /** テスト用のCache */
    @Mock
    private CacheHelper<String, String>         cacheHelper;

    /** テスト用のCache */
    @Mock
    private RemoteCache<String, String>         mockCache;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        TupleCacheMapper<String, String> mapper = new SimpleCacheMapper<>();
        this.target = new InfinispanStoreBolt<>("", "", mapper);

        Whitebox.setInternalState(this.target, "collector", this.mockCollector);
        Whitebox.setInternalState(this.target, "cacheHelper", this.cacheHelper);

        Mockito.when(this.cacheHelper.getCache()).thenReturn(this.mockCache);
    }

    /**
     * Keyの変換に失敗した場合、キャッシュに値の保存がされないことの確認を行う。
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test キャッシュに値の保存がされないこと
     *    condition::  Keyの変換に失敗
     *    result:: キャッシュに値の保存がされないこと
     */
    @Test
    public void testExecute_Key変換失敗()
    {
        // 準備
        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_KEY)).thenThrow(
                new IllegalArgumentException(FieldName.MESSAGE_KEY + " does not exist"));

        // 実施
        this.target.execute(this.mockTuple);

        // 検証
        Mockito.verify(this.mockCache, Mockito.never()).put(anyString(), anyString());
        Mockito.verify(this.mockCollector).ack(this.mockTuple);
    }

    /**
     * Valueの変換に失敗した場合、キャッシュに値の保存がされないことの確認を行う。
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test キャッシュに値の保存がされないこと
     *    condition::  Valueの変換に失敗
     *    result:: キャッシュに値の保存がされないこと
     */
    @Test
    public void testExecute_Value変換失敗()
    {
        // 準備
        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_KEY)).thenReturn("MessageKey");
        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_VALUE)).thenThrow(
                new IllegalArgumentException(FieldName.MESSAGE_VALUE + " does not exist"));

        // 実施
        this.target.execute(this.mockTuple);

        // 検証
        Mockito.verify(this.mockCache, Mockito.never()).put(anyString(), anyString());
        Mockito.verify(this.mockCollector).ack(this.mockTuple);
    }

    /**
     * キャッシュへの保存が失敗した場合、Ackが返ること。
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test Ackが返ること
     *    condition::  キャッシュへの保存が失敗した場合
     *    result:: Ackが返ること
     */
    @Test
    public void testExecute_保存失敗()
    {
        // 準備
        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_KEY)).thenReturn("MessageKey");
        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_VALUE)).thenReturn(
                "MessageValue");
        Mockito.when(this.mockCache.put("MessageKey", "MessageValue")).thenThrow(
                new HotRodClientException("Put failed."));

        // 実施
        this.target.execute(this.mockTuple);

        // 検証
        Mockito.verify(this.mockCollector).ack(this.mockTuple);
    }

    /**
     * キャッシュへの保存が成功した場合、保存後メソッドが呼び出されること。
     *
     * @target {@link InfinispanStoreBolt#execute(Tuple)}
     * @test 保存後メソッドが呼び出されること
     *    condition::  キャッシュへの保存が成功した場合
     *    result:: 保存後メソッドが呼び出されること
     */
    @Test
    public void testExecute_保存成功()
    {
        // 準備
        this.target = Mockito.spy(this.target);

        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_KEY)).thenReturn("MessageKey");
        Mockito.when(this.mockTuple.getValueByField(FieldName.MESSAGE_VALUE)).thenReturn(
                "MessageValue");

        // 実施
        this.target.execute(this.mockTuple);

        // 検証
        Mockito.verify(this.target).onStoreAfter(this.mockTuple, "MessageKey", "MessageValue");
        Mockito.verify(this.mockCollector).ack(this.mockTuple);
    }
}
