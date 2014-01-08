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
package acromusashi.stream.component.cassandra.bolt;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.CassandraBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;

/**
 * Cassandraに受信Tupleを書き込むBolt<br>
 * 受信したTupleをCassandraに書き込むのみで、下流への送信は行わない。
 * 
 * @author kimura
 *
 * @param <K> RowKeyの型
 * @param <C> カラム名の型
 * @param <V> カラム値の型
 */
public class CassandraStoreBolt<K, C, V> extends CassandraBolt<K, C, V> implements IRichBolt
{
    /** serialVersionUID */
    private static final long         serialVersionUID = -1151860639847216951L;

    /** logger */
    private static final Logger       logger           = LoggerFactory.getLogger(CassandraStoreBolt.class);

    /** OutputCollector */
    protected transient OutputCollector collector;

    /** 設定値を保持するキー値 */
    protected String                  clientConfigKey;

    /**
     * 設定値キーとCassandraへのマッピング定義を指定してインスタンスを生成する。
     * 
     * @param clientConfigKey 設定値キー
     * @param tupleMapper Cassandraへのマッピング定義
     */
    public CassandraStoreBolt(String clientConfigKey, TupleMapper<K, C, V> tupleMapper)
    {
        super(clientConfigKey, tupleMapper);
        this.clientConfigKey = clientConfigKey;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        Map targetConfig = stormConf;
        // prepareメソッドで渡されるConfig、及び内部のMapはClojureが生成するMapのためイミュータブルなMapとなっている。
        // そのため、新たなMapを生成し、コピーして使用している。
        Map<String, Object> baseConfig = (Map<String, Object>) targetConfig.get(this.clientConfigKey);
        // Cassandraのタイムアウト値が設定されている場合はCassandraの接続設定を生成し、親クラスに渡す。
        if (baseConfig.containsKey("cassandra.connection.timeout") == true)
        {
            targetConfig = new HashMap();
            targetConfig.putAll(stormConf);
            Map<String, Object> cassandraConfig = new HashMap<String, Object>();
            cassandraConfig.putAll(baseConfig);
            int connectionTimeout = Integer.parseInt(baseConfig.get("cassandra.connection.timeout").toString());
            ConnectionPoolConfiguration poolConf = new ConnectionPoolConfigurationImpl(
                    "MyConnectionPool").setConnectTimeout(connectionTimeout).setMaxTimeoutWhenExhausted(
                    connectionTimeout);
            cassandraConfig.put(AstyanaxClient.ASTYANAX_CONNECTION_POOL_CONFIGURATION, poolConf);
            targetConfig.put(this.clientConfigKey, cassandraConfig);
        }

        super.prepare(targetConfig, context);
        this.collector = collector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        try
        {
            super.writeTuple(input, this.tupleMapper);
        }
        catch (Exception ex)
        {
            String messageFormat = "Tuple write failed. InputTuple={0}";
            String errorMessage = MessageFormat.format(messageFormat, input.toString());
            logger.warn(errorMessage, ex);
        }

        getCollector().ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // 下流には送信しないため、何もしない
    }

    /**
     * Get OutputCollector.
     * 
     * @return OutputCollector
     */
    protected OutputCollector getCollector()
    {
        return this.collector;
    }
}
