package acromusashi.stream.config;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import acromusashi.stream.util.ResourceResolver;

/**
 * ConfigFileWatcherのテストクラス
 * 
 * @author kimura
 */
public class ConfigFileWatcherTest
{
    /**
     * 監視対象のファイルが存在する場合に最終更新時刻が設定されることを確認する。
     * 
     * @target {@link ConfigFileWatcher#init()}
     * @test 最終更新時刻が設定されること
     *    condition::  監視対象のファイルが存在する場合に初期化
     *    result:: 最終更新時刻が設定されること
     */
    @Test
    public void testInit_対象ファイル存在()
    {
        // 準備
        String filePath = ResourceResolver.resolve("ConfigFileWatcherTest_ReloadConfirm.yaml").getAbsolutePath();

        // 実施
        ConfigFileWatcher watcher = new ConfigFileWatcher(filePath, 30L);
        watcher.init();

        // 検証
        assertTrue(watcher.lastModifytime > 0);
    }

    /**
     * 監視対象のファイルが存在しない場合に最終更新時刻が設定されないことを確認する。
     * 
     * @target {@link ConfigFileWatcher#init()}
     * @test 最終更新時刻が設定されないこと
     *    condition::  監視対象のファイルが存在しない場合に初期化
     *    result:: 最終更新時刻が設定されないこと
     */
    @Test
    public void testInit_対象ファイル未存在()
    {
        // 準備
        String filePath = "NotFound";

        // 実施
        ConfigFileWatcher watcher = new ConfigFileWatcher(filePath, 30L);
        watcher.init();

        // 検証
        assertTrue(watcher.lastModifytime == 0);
    }

    /**
     * 前回読み込み時刻から時間が経過していない場合、読み込みが行われないことを確認する。
     * 
     * @target {@link ConfigFileWatcher#readIfUpdated()}
     * @test 読み込みが行われないこと
     *    condition::  前回読み込み時刻から時間が経過していない場合
     *    result:: 読み込みが行われないこと
     */
    @Test
    public void testReadIfUpdated_前回確認時刻から時間未経過() throws IOException
    {
        // 準備
        String filePath = ResourceResolver.resolve("ConfigFileWatcherTest_ReloadConfirm.yaml").getAbsolutePath();
        ConfigFileWatcher watcher = new ConfigFileWatcher(filePath, 30L);
        watcher = Mockito.spy(watcher);
        Mockito.doReturn(1000L).when(watcher).getNowTime();
        watcher.init();
        Mockito.doReturn(2000L).when(watcher).getNowTime();

        // 実施
        Map<String, Object> actual = watcher.readIfUpdated();

        // 検証
        assertNull(actual);
        assertThat(watcher.lastWatchTime, equalTo(Long.valueOf(1000L)));
    }

    /**
     * 前回読み込み時刻から時間が経過しているが、ファイルが存在しない場合、読み込みが行われないことを確認する。
     * 
     * @target {@link ConfigFileWatcher#readIfUpdated()}
     * @test 読み込みが行われないこと
     *    condition::  前回読み込み時刻から時間が経過しているが、ファイルが存在しない場合
     *    result:: 読み込みが行われないこと
     */
    @Test
    public void testReadIfUpdated_ファイル未存在() throws IOException
    {
        // 準備
        String filePath = "NotFound";
        ConfigFileWatcher watcher = new ConfigFileWatcher(filePath, 30L);
        watcher = Mockito.spy(watcher);
        Mockito.doReturn(1000L).when(watcher).getNowTime();
        watcher.init();
        Mockito.doReturn(300000L).when(watcher).getNowTime();

        // 実施
        Map<String, Object> actual = watcher.readIfUpdated();

        // 検証
        assertNull(actual);
        assertThat(watcher.lastWatchTime, equalTo(Long.valueOf(300000L)));
    }

    /**
     * 前回読み込み時刻から時間が経過しているが、ファイルが更新されていない場合、読み込みが行われないことを確認する。
     * 
     * @target {@link ConfigFileWatcher#readIfUpdated()}
     * @test 読み込みが行われないこと
     *    condition::  前回読み込み時刻から時間が経過しているが、ファイルが更新されていない場合
     *    result:: 読み込みが行われないこと
     */
    @Test
    public void testReadIfUpdated_ファイル未更新() throws IOException
    {
        // 準備
        String filePath = ResourceResolver.resolve("ConfigFileWatcherTest_ReloadConfirm.yaml").getAbsolutePath();
        ConfigFileWatcher watcher = new ConfigFileWatcher(filePath, 30L);
        watcher = Mockito.spy(watcher);
        Mockito.doReturn(1000L).when(watcher).getNowTime();
        watcher.init();
        Mockito.doReturn(300000L).when(watcher).getNowTime();

        // 実施
        Map<String, Object> actual = watcher.readIfUpdated();

        // 検証
        assertNull(actual);
        assertThat(watcher.lastWatchTime, equalTo(Long.valueOf(300000L)));
    }

    /**
     * 前回読み込み時刻から時間が経過しており、ファイルが更新されている場合、読み込みが行われることを確認する。
     * 
     * @target {@link ConfigFileWatcher#readIfUpdated()}
     * @test 読み込みが行われること
     *    condition::  前回読み込み時刻から時間が経過しており、ファイルが更新されている場合
     *    result:: 読み込みが行われること
     */
    @Test
    public void testReadIfUpdated_ファイル更新() throws IOException
    {
        // 準備
        String filePath = ResourceResolver.resolve("ConfigFileWatcherTest_ReloadConfirm.yaml").getAbsolutePath();
        ConfigFileWatcher watcher = new ConfigFileWatcher(filePath, 30L);
        watcher = Mockito.spy(watcher);
        Mockito.doReturn(1000L).when(watcher).getNowTime();
        watcher.init();

        watcher.lastModifytime = 1000L;
        Mockito.doReturn(300000L).when(watcher).getNowTime();

        // 実施
        Map<String, Object> actual = watcher.readIfUpdated();

        // 検証
        assertNotNull(actual);
        assertThat(watcher.lastWatchTime, equalTo(Long.valueOf(300000L)));
        assertThat(watcher.lastModifytime, is(not(equalTo(Long.valueOf(1000L)))));
    }
}
