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
package acromusashi.stream.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.trace.KeyHistoryRecorder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * メッセージ中のキー情報履歴を保持する機能を持つBaseSpoutクラス。<br>
 * 本クラスを継承したSpoutを使用することで下記の機能を利用可能。<br>
 * <ol>
 * <li>メッセージ中のキー情報を履歴として保持</li>
 * </ol>
 */
public abstract class KeyTraceBaseSpout extends BaseConfigurationSpout
{
    /** serialVersionUID */
    private static final long serialVersionUID = -3966364804089682434L;

    /** タスクID */
    protected String          taskId;

    /**
     * SpoutがWorkerプロセスに展開された後に実行される初期化メソッド。<br>
     * Stormクラスタから受け取ったオブジェクト群の設定と、TaskIdの算出を行う。
     *
     * @param conf Storm設定オブジェクト
     * @param context Topologyコンテキスト
     * @param collector Collectorオブジェクト
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        super.open(conf, context, collector);
        // TaskIdを算出し、フィールドに保持
        this.taskId = context.getThisComponentId() + "_" + context.getThisTaskId();
        onOpen(conf, context);
    }

    /**
     * Spoutの初期化処理を行う。<br>
     * リソースの初期化など起動時に必要な処理を記述する。
     *
     * @param conf Storm設定オブジェクト
     * @param context Topologyコンテキスト
     */
    @SuppressWarnings("rawtypes")
    public abstract void onOpen(Map conf, TopologyContext context);

    /**
     * Stormから継続して実行される次のメッセージを取得するメソッド。<br>
     * Spoutが動作中は本メソッドが延々実行される。
     */
    @Override
    public void nextTuple()
    {
        onNextTuple();
    }

    /**
     * 次のメッセージをメッセージソースから取得する。
     */
    public abstract void onNextTuple();

    /**
     * 継承クラス側で指定されたフィールドリストに追加してキーの履歴情報を示すキーを追加して返す。
     *
     * @param declarer フィールド取得オブジェクト
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        List<String> baseList = new ArrayList<String>();
        baseList.add(FieldName.KEY_HISTORY);
        baseList.addAll(getDeclareOutputFields());
        declarer.declare(new Fields(baseList));
    }

    /**
     * メッセージに設定するフィールドリストを取得する。
     *
     * @return メッセージに設定するフィールドリスト
     */
    public abstract List<String> getDeclareOutputFields();

    /**
     * MessageKey(キー情報履歴として出力する値)、MessageId(Stormのメッセージ処理失敗検知機構に指定する値)として同一の値を指定して下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用する。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用する。</li>
     * </ol>
     *
     * @param message 送信メッセージ
     * @param messageKeyId メッセージを一意に特定するためのキー情報、Stormのメッセージ処理失敗検知機構を利用する際のID
     */
    protected void emit(List<Object> message, Object messageKeyId)
    {
        // メッセージにキー情報を記録する。
        KeyHistoryRecorder.recordKeyHistory(message, messageKeyId);
        this.getCollector().emit(message, messageKeyId);
    }

    /**
     * MessageKey(キー情報履歴として出力する値)、MessageId(Stormのメッセージ処理失敗検知機構に指定する値)を指定せずに下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用しない。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用しない。</li>
     * </ol>
     *
     * @param message 送信メッセージ
     */
    protected void emitWithNoKeyId(List<Object> message)
    {
        KeyHistoryRecorder.recordKeyHistory(message);
        this.getCollector().emit(message);
    }

    /**
     * MessageKey(キー情報履歴として出力する値)のみを指定して下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用する。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用しない。</li>
     * </ol>
     *
     * @param message 送信メッセージ
     * @param messageKey メッセージを一意に特定するためのキー情報
     */
    protected void emitWithKeyOnly(List<Object> message, Object messageKey)
    {
        // メッセージにキー情報を記録する。
        KeyHistoryRecorder.recordKeyHistory(message, messageKey);
        this.getCollector().emit(message);
    }

    /**
     * MessageKey(キー情報履歴として出力する値)、MessageId(Stormのメッセージ処理失敗検知機構に指定する値)を個別に指定して下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用する。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用する。</li>
     * </ol>
     *
     * @param message 送信メッセージ
     * @param messageKey メッセージを一意に特定するためのキー情報
     * @param messageId Stormのメッセージ処理失敗検知機構を利用する際のID
     *
     */
    protected void emitWithDifferentKeyId(List<Object> message, Object messageKey, Object messageId)
    {
        // メッセージにキー情報を記録する。
        KeyHistoryRecorder.recordKeyHistory(message, messageKey);
        this.getCollector().emit(message, messageId);
    }
}
