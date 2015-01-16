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

import java.util.List;
import java.util.Map;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.trace.KeyHistoryRecorder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Lists;

/**
 * AcroMUSASHI Stream's basis spout class<br>
 * Spout that inherit this class has following function.<br>
 * <ol>
 * <li>Has message's key history.</li>
 * </ol>
 *
 * @author kimura
 */
public abstract class AmBaseSpout extends AmConfigurationSpout
{
    /** serialVersionUID */
    private static final long serialVersionUID = -3966364804089682434L;

    /** Task id. */
    protected String          taskId;

    /**
     * Initialize method called after extracted for worker processes.<br>
     * <br>
     * Initialize task id.
     *
     * @param conf Storm configuration
     * @param context Topology context
     * @param collector SpoutOutputCollector
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        super.open(conf, context, collector);

        this.taskId = context.getThisComponentId() + "_" + context.getThisTaskId();
        onOpen(conf, context);
    }

    /**
     * Initialize method for individual spout.<br>
     * <br>
     * Describe processing at startup, such as initialization of resources.
     *
     * @param conf Storm configuration
     * @param context Topology context
     */
    @SuppressWarnings("rawtypes")
    public abstract void onOpen(Map conf, TopologyContext context);

    /**
     * Continually called method by Storm that gets next message.<br>
     * <br>
     * When spout is running, this method is endlessly called.
     */
    @Override
    public void nextTuple()
    {
        onNextTuple();
    }

    /**
     * Get next message from message source.
     */
    public abstract void onNextTuple();

    /**
     * Declare output fields and streams.<br>
     * Declare fields are following.<br>
     * <ol>
     * <li>messageKey   : Groupingkey if exists.</li>
     * <li>keyHistory   : Key history.</li>
     * <li>messageValue : Message value.</li>
     * </ol>
     * Streams are "default" and user setting streams.
     *
     * @param declarer declarer object
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        List<String> fields = Lists.newArrayList(FieldName.MESSAGE_KEY, FieldName.KEY_HISTORY,
                FieldName.MESSAGE_VALUE);

        // Declare default stream
        declarer.declare(new Fields(fields));

        for (String stream : getOutputStreams())
        {
            declarer.declareStream(stream, new Fields(fields));
        }
    }

    /**
     * Define downstream streams.<br>
     * If it needs other than default stream, inherit this method and return extra streams.
     *
     * @return Extra streams
     */
    protected List<String> getOutputStreams()
    {
        return Lists.newArrayList();
    }

    /**
     * Use same value used by MessageKey(Use key history's value), MessageId(Id identify by storm). And send message to downstream component.<br>
     * User following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault tolerant  function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKeyId MessageKey(Use key history's value), MessageId(Id identify by storm), and GroupingKey
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
