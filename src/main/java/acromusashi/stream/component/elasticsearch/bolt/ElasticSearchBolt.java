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
package acromusashi.stream.component.elasticsearch.bolt;

import java.text.MessageFormat;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.BaseConfigurationBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * ElasticSearchに対してクエリを投入するBolt
 *
 * @author kimura
 */
public class ElasticSearchBolt extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long   serialVersionUID  = 4987555107871741041L;

    /** logger */
    private static final Logger logger            = LoggerFactory.getLogger(ElasticSearchBolt.class);

    /** デフォルトポート値 */
    private static final int    DEFAULT_PORT      = 9300;

    /** クラスタ名称 */
    protected String            clusterName;

    /** ElasticSearchの投入先。「host1:port1;host2:port2;host3:port3...」という形式で定義 */
    protected String            servers;

    /** TupleをIndex Requestに変換するコンバータ */
    protected EsTupleConverter  converter;

    /** ElasticSearchClient */
    protected transient Client  client;

    /**
     * Converterを指定してインスタンスを生成する。
     *
     * @param converter TupleをIndex Requestに変換するコンバータ
     */
    public ElasticSearchBolt(EsTupleConverter converter)
    {
        this.converter = converter;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes", "resource"})
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        // cleanupメソッドは呼ばれないため、リソースクローズはできない。
        // だが、1インスタンスのみしか生成されないため、リークはしない。
        super.prepare(stormConf, context, collector);
        // ElasticSearchClientを初期化
        // Serversは「host1:port1;host2:port2;host3:port3...」形式のため分割して生成
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name",
                this.clusterName).build();
        TransportClient transportClient = new TransportClient(settings);
        for (String server : this.servers.split(";"))
        {
            String[] components = server.trim().split(":");
            String host = components[0];
            int port = DEFAULT_PORT;
            if (components.length > 1)
            {
                port = Integer.parseInt(components[1]);
            }

            transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(
                    host, port));
        }

        this.client = transportClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        String documentId = null;
        String indexName = null;
        String typeName = null;
        String document = null;

        try
        {
            documentId = this.converter.convertToId(input);
            indexName = this.converter.convertToIndex(input);
            typeName = this.converter.convertToType(input);
            document = this.converter.convertToDocument(input);

            IndexResponse response = this.client.prepareIndex(indexName, typeName, documentId).setSource(
                    document).execute().actionGet();

            if (logger.isDebugEnabled() == true)
            {
                String logFormat = "Document Indexed. Id={0}, Type={1}, Index={2}, Version={3}";
                logger.debug(MessageFormat.format(logFormat, response.getId(), typeName, indexName,
                        response.getVersion()));
            }
        }
        catch (Exception ex)
        {
            String logFormat = "Document Index failed. Dispose Tuple. Id={0}, Type={1}, Index={2}";
            logger.warn(MessageFormat.format(logFormat, documentId, typeName, indexName), ex);
        }

        getCollector().ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // 下流に送信を行わないため、未設定
    }

    /**
     * @param clusterName the clusterName to set
     */
    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    /**
     * @param servers the servers to set
     */
    public void setServers(String servers)
    {
        this.servers = servers;
    }
}
