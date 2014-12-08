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
package acromusashi.stream.bolt;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.trace.KeyHistory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * メッセージ中のキー情報履歴を保持する機能を持つBaseBoltクラス。<br>
 * 本クラスを継承したBoltを使用することで下記の機能を利用可能。<br>
 * <ol>
 * <li>メッセージ中のキー情報を履歴として保持</li>
 * </ol>
 */
public abstract class KeyTraceBaseBolt extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long   serialVersionUID = 1546366821557201305L;

    /** ロガー */
    private static final Logger logger           = LoggerFactory.getLogger(KeyTraceBaseBolt.class);

    /** タスクID */
    protected String            taskId;

    /** Executeメソッドで処理中のメッセージが保持するキー情報履歴 */
    private KeyHistory          executingKeyHistory;

    /** メッセージ処理時に応答を返したかどうかを示すフラグ */
    private boolean             isResponsed;

    /**
     * BoltがWorkerプロセスに展開された後に実行される初期化メソッド。<br>
     * Stormクラスタから受け取ったオブジェクト群の設定と、TaskIdの算出を行う。
     *
     * @param stormConf Storm設定オブジェクト
     * @param context Topologyコンテキスト
     * @param collector Collectorオブジェクト
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);
        // TaskIdを算出し、フィールドに保持
        this.taskId = context.getThisComponentId() + "_" + context.getThisTaskId();
        onPrepare(stormConf, context);
    }

    /**
     * Boltの初期化処理を行う。<br>
     * リソースの初期化など起動時に必要な処理を記述する。
     *
     * @param stormConf Storm設定オブジェクト
     * @param context Topologyコンテキスト
     */
    @SuppressWarnings("rawtypes")
    public abstract void onPrepare(Map stormConf, TopologyContext context);

    /**
     * メッセージを受信した際に実行される。
     *
     * @param input 受信メッセージ
     */
    @Override
    public void execute(Tuple input)
    {
        // メッセージ処理中を示すステータスを初期化する。
        try
        {
            this.executingKeyHistory = (KeyHistory) input.getValueByField(FieldName.KEY_HISTORY);
        }
        catch (IllegalArgumentException iaex)
        {
            // TupleにKeyHistoryInfoが含まれていない場合はWARNログを出力し、空のKeyHistoryInfoを生成する。
            String logFormat = "Message has no keyhistory. Default blank keyhistory generate and continue. : Message={0}, KeyHistoryField={1}";
            logger.warn(MessageFormat.format(logFormat, input, FieldName.KEY_HISTORY), iaex);

            this.executingKeyHistory = new KeyHistory();
        }

        this.isResponsed = false;

        // メッセージ処理を行う。
        // onExecuteメソッドから例外が投げられた場合はackを返す必要はないため、本クラス内でハンドリングは行わない。
        try
        {
            onExecute(input);
        }
        finally
        {
            // 処理中Tupleは例外の発生有無に関わらずクリアする。
            clearExecuteStatus();
        }

        // 応答を返していない場合は自動でackを返す。
        if (this.isResponsed == false)
        {
            getCollector().ack(input);
        }
    }

    /**
     * メッセージの実行中状態を解除
     */
    protected void clearExecuteStatus()
    {
        this.executingKeyHistory = null;
    }

    /**
     * メッセージ受信時の処理を記述する。
     *
     * @param input 受信メッセージ
     */
    public abstract void onExecute(Tuple input);

    /**
     * 継承クラス側で指定されたフィールドリストに追加してキーの履歴情報を示すキーを追加して返す。
     *
     * @param declarer フィールド取得オブジェクト
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        List<String> baseList = new ArrayList<>();
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
     * 親メッセージ、MessageKey(キー情報履歴として出力する値)を指定せずに下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用しない。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用しない。</li>
     * </ol>
     *
     * @param message 送信メッセージ
     */
    protected void emitWithNoAnchorKey(List<Object> message)
    {
        message.add(0, this.executingKeyHistory);
        getCollector().emit(message);
    }

    /**
     * 親メッセージのみ指定して下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用しない。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用しており、かつ下流のBoltでTupleの処理に失敗した場合Spoutから取得したTupleを失敗と扱う。</li>
     * </ol>
     *
     * @param anchor 親メッセージ
     * @param message 送信メッセージ
     */
    protected void emitWithNoKey(Tuple anchor, List<Object> message)
    {
        message.add(0, this.executingKeyHistory);
        getCollector().emit(anchor, message);
    }

    /**
     * MessageKey(キー情報履歴として出力する値)のみ指定して下流コンポーネントへメッセージを送信する。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用する。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用しない。</li>
     *
     * @param message 送信メッセージ
     * @param messageKey メッセージを一意に特定するためのキー情報
     */
    protected void emitWithNoAnchor(List<Object> message, Object messageKey)
    {
        // メッセージを分割した場合に対応するため、KeyHistoryを複製して用いている。
        KeyHistory newHistory = this.executingKeyHistory.createDeepCopy();
        newHistory.addKey(messageKey.toString());
        message.add(0, newHistory);
        getCollector().emit(message);
    }

    /**
     * 親メッセージ、MessageKey(キー情報履歴として出力する値)を指定してTupleのemitを行う。<br>
     * 下記の条件の時に用いること。
     * <ol>
     * <li>本クラスの提供するキー情報履歴を使用する。</li>
     * <li>Stormのメッセージ処理失敗検知機構を使用しており、かつ下流のBoltでTupleの処理に失敗した場合Spoutから取得したTupleを失敗と扱う。</li>
     * </ol>
     *
     * @param anchor 親メッセージ
     * @param message 送信メッセージ
     * @param messageKey メッセージを一意に特定するためのキー情報
     */
    protected void emit(Tuple anchor, List<Object> message, Object messageKey)
    {
        // メッセージを分割した場合に対応するため、KeyHistoryを複製して用いている。
        KeyHistory newHistory = this.executingKeyHistory.createDeepCopy();
        newHistory.addKey(messageKey.toString());
        message.add(0, newHistory);
        getCollector().emit(anchor, message);
    }

    /**
     * 対象の親メッセージに対してackを返す。
     *
     * @param tuple 親メッセージ
     */
    protected void ack(Tuple tuple)
    {
        this.isResponsed = true;
        getCollector().ack(tuple);
    }

    /**
     * 対象の親メッセージに対してfailを返す。
     *
     * @param tuple 親メッセージ
     */
    protected void fail(Tuple tuple)
    {
        this.isResponsed = true;
        getCollector().fail(tuple);
    }
}
