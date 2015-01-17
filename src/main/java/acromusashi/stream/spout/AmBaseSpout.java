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
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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

    /** Record key history flag. */
    protected boolean         recordHistory    = true;

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
     * <li>messageValue : Message value.</li>
     * </ol>
     * Streams are "default" and user setting streams.
     *
     * @param declarer declarer object
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        List<String> fields = Lists.newArrayList(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE);

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
     * @param recordHistory the recordHistory to set
     */
    public void setRecordHistory(boolean recordHistory)
    {
        this.recordHistory = recordHistory;
    }

    /**
     * Use MessageKey(Use key history's value), MessageId(Id identify by storm).<br>
     * Send message to downstream component.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * <li>MessageKey and MessageId are different value.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param messageId MessageId(Id identify by storm)
     */
    protected void emit(StreamMessage message, Object messageKey, Object messageId)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(new Values("", message), messageId);
    }

    /**
     * Use MessageKey(Use key history's value), MessageId(Id identify by storm).<br>
     * Send message to downstream component with grouping key.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * <li>MessageKey and MessageId are different value.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param messageId MessageId(Id identify by storm)
     * @param groupingKey grouping key
     */
    protected void emitWithGrouping(StreamMessage message, Object messageKey, Object messageId,
            String groupingKey)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(new Values(groupingKey, message), messageId);
    }

    /**
     * Use MessageKey(Use key history's value), MessageId(Id identify by storm).<br>
     * Send message to downstream component with streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * <li>MessageKey and MessageId are different value.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param messageId MessageId(Id identify by storm)
     * @param streamId streamId
     */
    protected void emitWithStream(StreamMessage message, Object messageKey, Object messageId,
            String streamId)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(streamId, new Values("", message), messageId);
    }

    /**
     * Use MessageKey(Use key history's value), MessageId(Id identify by storm).<br>
     * Send message to downstream component with grouping key and streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * <li>MessageKey and MessageId are different value.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param messageId MessageId(Id identify by storm)
     * @param groupingKey grouping key
     * @param streamId streamId
     */
    protected void emitWithGroupingStream(StreamMessage message, Object messageKey,
            Object messageId, String groupingKey, String streamId)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(streamId, new Values(groupingKey, message), messageId);
    }

    /**
     * Not use this class's key history function, and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     */
    protected void emitWithNoKeyId(StreamMessage message)
    {
        this.getCollector().emit(new Values("", message));
    }

    /**
     * Not use this class's key history function, and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component with grouping key.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param groupingKey grouping key
     */
    protected void emitWithNoKeyIdAndGrouping(StreamMessage message, String groupingKey)
    {
        this.getCollector().emit(new Values(groupingKey, message));
    }

    /**
     * Not use this class's key history function, and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component with streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param streamId streamId
     */
    protected void emitWithNoKeyIdAndStream(StreamMessage message, String streamId)
    {
        this.getCollector().emit(streamId, new Values("", message));
    }

    /**
     * Not use this class's key history function, and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component with grouping key and streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param groupingKey grouping key
     * @param streamId streamId
     */
    protected void emitWithNoKeyIdAndGroupingStream(StreamMessage message, String groupingKey,
            String streamId)
    {
        this.getCollector().emit(streamId, new Values(groupingKey, message));
    }

    /**
     * Use only MessageKey(Use key history's value) and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     */
    protected void emitWithOnlyKey(StreamMessage message, Object messageKey)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(new Values("", message));
    }

    /**
     * Use only MessageKey(Use key history's value) and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component with grouping key.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param groupingKey grouping key
     */
    protected void emitWithOnlyKeyAndGrouping(StreamMessage message, Object messageKey,
            String groupingKey)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(new Values(groupingKey, message));
    }

    /**
     * Use only MessageKey(Use key history's value) and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component with streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param streamId streamId
     */
    protected void emitWithOnlyKeyAndStream(StreamMessage message, Object messageKey,
            String streamId)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(streamId, new Values("", message));
    }

    /**
     * Use only MessageKey(Use key history's value) and not use MessageId(Id identify by storm).<br>
     * Send message to downstream component with grouping key and streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param groupingKey grouping key
     * @param streamId streamId
     */
    protected void emitWithOnlyKeyAndGroupingStream(StreamMessage message, Object messageKey,
            String groupingKey, String streamId)
    {
        if (this.recordHistory)
        {
            message.getHeader().addHistory(messageKey.toString());
        }

        this.getCollector().emit(streamId, new Values(groupingKey, message));
    }
}
