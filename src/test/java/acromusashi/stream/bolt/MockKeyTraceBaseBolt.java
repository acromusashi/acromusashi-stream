/*
 * Copyright (C) 2013 NTT DATA Corporation
 * All rights reserved
 */
package acromusashi.stream.bolt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * KeyTraceBaseBolt検証用のモッククラス
 */
public class MockKeyTraceBaseBolt extends KeyTraceBaseBolt
{

    /** serialVersionUID */
    private static final long serialVersionUID = -1271953207061271186L;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void onPrepare(Map stormConf, TopologyContext context)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecute(Tuple input)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getDeclareOutputFields()
    {
        return Arrays.asList("Key", "Message");
    }
}
