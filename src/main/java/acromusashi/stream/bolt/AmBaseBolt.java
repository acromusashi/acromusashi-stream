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

import java.util.List;
import java.util.Map;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.trace.KeyHistory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

/**
 * AcroMUSASHI Stream's basis bolt class<br>
 * Spout that inherit this class has following function.<br>
 * <ol>
 * <li>Has message's key history.</li>
 * </ol>
 *
 * @author kimura
 */
public abstract class AmBaseBolt extends AmConfigurationBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 1546366821557201305L;

    /** Task id. */
    protected String          taskId;

    /** Executing message's key history */
    private KeyHistory        executingKeyHistory;

    /** Message acked flag. */
    private boolean           responsed;

    /** Record key history flag. */
    protected boolean         recordHistory    = true;

    /**
     * Initialize method called after extracted for worker processes.<br>
     * <br>
     * Initialize task id.
     *
     * @param stormConf Storm configuration
     * @param context Topology context
     * @param collector SpoutOutputCollector
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);

        this.taskId = context.getThisComponentId() + "_" + context.getThisTaskId();
        onPrepare(stormConf, context);
    }

    /**
     * Initialize method for individual bolt.<br>
     * <br>
     * Describe processing at startup, such as initialization of resources.
     *
     * @param stormConf Storm configuration
     * @param context Topology context
     */
    @SuppressWarnings("rawtypes")
    public abstract void onPrepare(Map stormConf, TopologyContext context);

    /**
     * Execute when receive message.
     *
     * @param received received message
     */
    @Override
    public void onMessage(StreamMessage received)
    {
        this.executingKeyHistory = received.getHeader().getHistory();
        this.responsed = false;

        // Execute message processing.
        // This class not handles exceptions.
        // Because if throws exception, this class not need ack.
        try
        {
            onExecute(received);
        }
        finally
        {
            clearExecuteStatus();
        }

        // If not responsed, auto ack
        if (this.responsed == false)
        {
            super.ack();
        }
    }

    /**
     * Clear message executing status.
     */
    protected void clearExecuteStatus()
    {
        this.executingKeyHistory = null;
    }

    /**
     * Execute when receive message.
     *
     * @param input received message
     */
    public abstract void onExecute(StreamMessage input);

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
     * Notify ack for inputed tuple.
     */
    @Override
    protected void ack()
    {
        super.ack();
        this.responsed = true;
    }

    /**
     * Notify fail for inputed tuple.
     */
    @Override
    protected void fail()
    {
        super.fail();
        this.responsed = true;
    }

    /**
     * Create keyhistory from original key history.<br>
     * Use following situation.
     * <ol>
     * <li>Not used current message key.</li>
     * <li>This class's key history function is not executed.</li>
     * </ol>
     *
     * @param history original key history
     * @return created key history
     */
    protected KeyHistory createKeyRecorededHistory(KeyHistory history)
    {
        KeyHistory result = null;

        if (history != null)
        {
            // For adjust message splited, use keyhistory's deepcopy.
            result = history.createDeepCopy();
        }

        return result;
    }

    /**
     * Create keyhistory from original key history and current message key.
     *
     * @param history original key history
     * @param messageKey current message key
     * @return created key history
     */
    protected KeyHistory createKeyRecorededHistory(KeyHistory history, Object messageKey)
    {
        KeyHistory result = null;

        if (history == null)
        {
            result = new KeyHistory();
        }
        else
        {
            // For adjust message splited, use keyhistory's deepcopy.
            result = history.createDeepCopy();
        }

        result.addKey(messageKey.toString());

        return result;
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), MessageKey(Use key history's value).<br>
     * Send message to downstream component.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     */
    protected void emit(StreamMessage message, Object messageKey)
    {
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(this.getExecutingTuple(), new Values("", message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), MessageKey(Use key history's value).<br>
     * Send message to downstream component with grouping key.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param groupingKey grouping key
     */
    protected void emitWithGrouping(StreamMessage message, Object messageKey, String groupingKey)
    {
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(this.getExecutingTuple(), new Values(groupingKey, message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), MessageKey(Use key history's value).<br>
     * Send message to downstream component with streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param streamId streamId
     */
    protected void emitWithStream(StreamMessage message, Object messageKey, String streamId)
    {
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(streamId, this.getExecutingTuple(), new Values("", message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), MessageKey(Use key history's value).<br>
     * Send message to downstream component with grouping key and streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param messageKey MessageKey(Use key history's value)
     * @param groupingKey grouping key
     * @param streamId streamId
     */
    protected void emitWithGroupingStream(StreamMessage message, Object messageKey,
            String groupingKey, String streamId)
    {
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(streamId, this.getExecutingTuple(), new Values(groupingKey, message));
    }

    /**
     * Not use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
     * Send message to downstream component.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Not use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     */
    protected void emitWithNoAnchorKey(StreamMessage message)
    {
        getCollector().emit(new Values("", message));
    }

    /**
     * Not use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
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
    protected void emitWithNoAnchorKeyAndGrouping(StreamMessage message, String groupingKey)
    {
        getCollector().emit(new Values(groupingKey, message));
    }

    /**
     * Not use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
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
    protected void emitWithNoAnchorKeyAndStream(StreamMessage message, String streamId)
    {
        getCollector().emit(streamId, new Values("", message));
    }

    /**
     * Not use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
     * Send message to downstream component with grouping key.<br>
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
    protected void emitWithNoAnchorKeyAndGroupingStream(StreamMessage message, String groupingKey,
            String streamId)
    {
        getCollector().emit(streamId, new Values(groupingKey, message));
    }

    /**
     * Use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
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
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(new Values("", message));
    }

    /**
     * Use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
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
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(new Values(groupingKey, message));
    }

    /**
     * Use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
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
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(streamId, new Values("", message));
    }

    /**
     * Use this class's key history function, and not use anchor function(child message failed. notify fail to parent message.).<br>
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
        KeyHistory newHistory = null;
        if (this.recordHistory)
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory, messageKey);
        }
        else
        {
            newHistory = createKeyRecorededHistory(this.executingKeyHistory);
        }

        message.getHeader().setHistory(newHistory);

        getCollector().emit(streamId, new Values(groupingKey, message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), and not use this class's key history function.<br>
     * Send message to downstream component.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     */
    protected void emitWithOnlyAnchor(StreamMessage message)
    {
        getCollector().emit(this.getExecutingTuple(), new Values("", message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), and not use this class's key history function.<br>
     * Send message to downstream component with grouping key.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param groupingKey grouping key
     */
    protected void emitWithOnlyAnchorAndGrouping(StreamMessage message, String groupingKey)
    {
        getCollector().emit(this.getExecutingTuple(), new Values(groupingKey, message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), and not use this class's key history function.<br>
     * Send message to downstream component with streamId.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param streamId streamId
     */
    protected void emitWithOnlyAnchorAndStream(StreamMessage message, String streamId)
    {
        getCollector().emit(streamId, this.getExecutingTuple(), new Values("", message));
    }

    /**
     * Use anchor function(child message failed. notify fail to parent message.), and not use this class's key history function.<br>
     * Send message to downstream component with grouping key.<br>
     * Use following situation.
     * <ol>
     * <li>Not use this class's key history function.</li>
     * <li>Use storm's fault detect function.</li>
     * </ol>
     *
     * @param message sending message
     * @param groupingKey grouping key
     * @param streamId streamId
     */
    protected void emitWithOnlyAnchorAndGroupingStream(StreamMessage message, String groupingKey,
            String streamId)
    {
        getCollector().emit(streamId, this.getExecutingTuple(), new Values(groupingKey, message));
    }
}
