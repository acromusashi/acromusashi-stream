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
package acromusashi.stream.component.kestrel.spout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.lag.kestrel.thrift.Item;

import org.apache.thrift7.TException;
import org.elasticsearch.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Sets;

/**
 * KestrelからThrftプロトコルでメッセージを取得し次Boltに送信するSpout<br>
 * <br>
 * storm-kestrel(https://github.com/nathanmarz/storm-kestrel)のKestrelThrftSpoutをベースに開発を行っている。
 * 
 * @author kimura
 */
public class KestrelSpout extends BaseRichSpout
{
    /** serialVersionUID */
    private static final long                 serialVersionUID          = -4508764195597194300L;

    /** 「接続失敗した際の次に取得するまでの待ち時間」デフォルト値 */
    public static final long                  DEFAULT_BLACKLIST_TIME_MS = 1000 * 60;

    /** 「Kestrelからメッセージを一度に取得する指定バッチサイズ」デフォルト値 */
    public static final int                   DEFAULT_BATCH_SIZE        = 4000;

    /** logger */
    private static final Logger               logger                    = LoggerFactory.getLogger(KestrelSpout.class);

    /** 接続失敗した際の次に取得するまでの待ち時間(ms) */
    protected long                            blackListTimeMs           = DEFAULT_BLACKLIST_TIME_MS;

    /** Kestrelからメッセージを一度に取得する指定バッチサイズ */
    protected int                             batchSize                 = DEFAULT_BATCH_SIZE;

    /** Kestrelからデータを取得する際のAckタイムアウト(ms) */
    protected int                             messageTimeoutMs;

    /** 接続先情報 */
    protected List<HostInfo>                  hostInfos                 = null;

    /** メッセージの取得対象となるキュー名称 */
    private String                            queueName                 = null;

    /** SpoutOutputCollector */
    private transient SpoutOutputCollector    collector;

    /** メッセージのデコードスキーム */
    private MultiScheme                       messageScheme;

    /** Kestrelの接続情報リスト */
    private transient List<KestrelClientInfo> clientInfoList;

    /** 現在最後に取得したKestrelのインデックス */
    private int                               emitIndex;

    /** Kestrelから取得したメッセージの蓄積用キャッシュ */
    private transient Queue<EmitItem>         emitBuffer;

    /**
     * ホスト名リスト、ポート番号、キュー名称、メッセージ変換用スキームを指定してインスタンスを生成する。
     * 
     * @param hostnames ホスト名リスト
     * @param port ポート番号
     * @param queueName キュー名称
     * @param scheme メッセージ変換用スキーム
     */
    public KestrelSpout(List<String> hostnames, int port, String queueName, Scheme scheme)
    {
        this(hostnames, port, queueName, new SchemeAsMultiScheme(scheme));
    }

    /**
     * 接続先文字列リスト、キュー名称、メッセージ変換用スキームを指定してインスタンスを生成する。<br>
     * 接続先文字列リストは「host:port」という形式で指定する。
     * 
     * @param hostStrs 接続先文字列リスト
     * @param queueName キュー名称
     * @param multiScheme メッセージ変換用スキーム
     */
    public KestrelSpout(List<String> hostStrs, String queueName, MultiScheme multiScheme)
    {
        if (hostStrs.isEmpty())
        {
            throw new IllegalArgumentException("Must configure at least one host");
        }
        this.hostInfos = new ArrayList<HostInfo>();
        for (String host : hostStrs)
        {
            String[] array = host.split(":");
            this.hostInfos.add(new HostInfo(array[0], Integer.parseInt(array[1])));
        }
        this.queueName = queueName;
        this.messageScheme = multiScheme;
    }

    /**
     * ホスト名リスト、ポート番号、キュー名称、メッセージ変換用スキームを指定してインスタンスを生成する。
     * 
     * @param hostnames ホスト名リスト
     * @param port ポート番号
     * @param queueName キュー名称
     * @param multiScheme メッセージ変換用スキーム
     */
    public KestrelSpout(List<String> hostnames, int port, String queueName, MultiScheme multiScheme)
    {
        if (hostnames.isEmpty())
        {
            throw new IllegalArgumentException("Must configure at least one host");
        }
        this.hostInfos = Lists.newArrayList();
        for (String hostname : hostnames)
        {
            this.hostInfos.add(new HostInfo(hostname, port));
        }
        this.queueName = queueName;
        this.messageScheme = multiScheme;
    }

    /**
     * ホスト名、ポート番号、キュー名称、メッセージ変換用スキームを指定してインスタンスを生成する。
     * 
     * @param hostname ホスト名
     * @param port ポート番号
     * @param queueName キュー名称
     * @param scheme メッセージ変換用スキーム
     */
    public KestrelSpout(String hostname, int port, String queueName, Scheme scheme)
    {
        this(hostname, port, queueName, new SchemeAsMultiScheme(scheme));
    }

    /**
     * ホスト名、ポート番号、キュー名称、メッセージ変換用スキームを指定してインスタンスを生成する。
     * 
     * @param hostname ホスト名
     * @param port ポート番号
     * @param queueName キュー名称
     * @param multiScheme メッセージ変換用スキーム
     */
    public KestrelSpout(String hostname, int port, String queueName, MultiScheme multiScheme)
    {
        this(Lists.newArrayList(hostname), port, queueName, multiScheme);
    }

    /**
     * ホスト名、ポート番号、キュー名称を指定してインスタンスを生成する。<br>
     * Kestrelから取得した際の変換は行わず、そのままByte列で返す。
     * 
     * @param hostname ホスト名
     * @param port ポート番号
     * @param queueName キュー名称
     */
    public KestrelSpout(String hostname, int port, String queueName)
    {
        this(hostname, port, queueName, new RawMultiScheme());
    }

    /**
     * ホスト名リスト、ポート番号、キュー名称を指定してインスタンスを生成する。<br>
     * Kestrelから取得した際の変換は行わず、そのままByte列で返す。
     * 
     * @param hostnames ホスト名リスト
     * @param port ポート番号
     * @param queueName キュー名称
     */
    public KestrelSpout(List<String> hostnames, int port, String queueName)
    {
        this(hostnames, port, queueName, new RawMultiScheme());
    }

    /**
     * 出力フィールド値を返す。
     * 
     * @return 出力フィールドリスト\\\
     */
    public Fields getOutputFields()
    {
        return this.messageScheme.getOutputFields();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        Number timeout = (Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
        this.messageTimeoutMs = (int) TimeUnit.SECONDS.toMillis(timeout.longValue());
        this.collector = collector;
        this.emitIndex = 0;
        this.clientInfoList = Lists.newArrayList();
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int myIndex = context.getThisTaskIndex();
        int numHosts = this.hostInfos.size();
        if (numTasks < numHosts)
        {
            for (HostInfo host : this.hostInfos)
            {
                this.clientInfoList.add(new KestrelClientInfo(host.getHost(), host.getPort()));
            }
        }
        else
        {
            HostInfo host = this.hostInfos.get(myIndex % numHosts);
            this.clientInfoList.add(new KestrelClientInfo(host.getHost(), host.getPort()));
        }

        this.emitBuffer = new LinkedList<EmitItem>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        for (KestrelClientInfo info : this.clientInfoList)
        {
            info.closeClient();
        }

        // Closing the client connection causes all the open reliable reads to be aborted.
        // Thus, clear our local buffer of these reliable reads.
        this.emitBuffer.clear();

        this.clientInfoList.clear();
    }

    /**
     * 指定したインデックスに対応したクライアント接続情報からメッセージを取得する。
     * 
     * @param index クライアント接続情報インデックス
     * @return 取得成功した場合true、失敗した場合false
     */
    public boolean bufferKestrelGet(int index)
    {
        KestrelClientInfo info = this.clientInfoList.get(index);

        long now = System.currentTimeMillis();
        if (now > info.blacklistTillTimeMs)
        {
            List<Item> items = null;
            try
            {
                items = info.getValidClient().get(this.queueName, DEFAULT_BATCH_SIZE, 0,
                        this.messageTimeoutMs);
            }
            catch (TException e)
            {
                blacklist(info, e);
                return false;
            }

            Set<Long> toAck = Sets.newHashSet();

            for (Item item : items)
            {
                Iterable<List<Object>> retItems = this.messageScheme.deserialize(item.get_data());

                if (retItems != null)
                {
                    for (List<Object> retItem : retItems)
                    {
                        EmitItem emitItem = generateEmitItem(retItem, new KestrelSourceId(index,
                                item.get_id()));

                        if (!this.emitBuffer.offer(emitItem))
                        {
                            throw new RuntimeException(
                                    "KestrelThriftSpout's Internal Buffer Enqeueue Failed.");
                        }
                    }

                }
                else
                {
                    toAck.add(item.get_id());
                }
            }

            if (toAck.size() > 0)
            {
                try
                {
                    info.getClient().confirm(this.queueName, toAck);
                }
                catch (TException e)
                {
                    blacklist(info, e);
                }
            }

            if (items.size() > 0)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Kestrelから取得可能なメッセージを取得する。
     */
    public void tryEachKestrelUntilBufferFilled()
    {
        for (int i = 0; i < this.clientInfoList.size(); i++)
        {
            int index = (this.emitIndex + i) % this.clientInfoList.size();
            if (bufferKestrelGet(index))
            {
                this.emitIndex = index;
                break;
            }
        }

        this.emitIndex = (this.emitIndex + 1) % this.clientInfoList.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nextTuple()
    {
        if (isRestricted() == false)
        {
            if (this.emitBuffer.isEmpty())
            {
                tryEachKestrelUntilBufferFilled();
            }

            EmitItem item = this.emitBuffer.poll();
            if (item != null)
            {
                this.collector.emit(item.getTuple(), item.getSourceId());
            }
        }
    }

    /**
     * Kestrelから取得した情報からEmit用のオブジェクトを生成する。
     * 
     * @param retItems Kestrelから取得した情報
     * @param sourceId Kestrel接続元情報
     * @return Emit用のオブジェクト
     */
    protected EmitItem generateEmitItem(List<Object> retItems, KestrelSourceId sourceId)
    {
        // If you needs custom tuple generation, override this method.
        EmitItem generatedItem = new EmitItem(retItems, sourceId);
        return generatedItem;
    }

    /**
     * 取得規制された状態かを返す。
     * 
     * @return 規制されていればtrue、規制されていなければfalse
     */
    protected boolean isRestricted()
    {
        // If you needs restrict state, override this method.
        return false;
    }

    /**
     * メッセージの取得失敗したクライアントに除外設定を行う。
     * 
     * @param info クライアント接続情報
     * @param t 発生例外
     */
    private void blacklist(KestrelClientInfo info, Throwable t)
    {
        logger.warn("Failed to read from Kestrel at " + info.host + ":" + info.port, t);

        //this case can happen when it fails to connect to Kestrel (and so never stores the connection)
        info.closeClient();
        info.blacklistTillTimeMs = System.currentTimeMillis() + DEFAULT_BLACKLIST_TIME_MS;

        int index = this.clientInfoList.indexOf(info);

        // we just closed the connection, so all open reliable reads will be aborted. empty buffers.
        for (Iterator<EmitItem> i = this.emitBuffer.iterator(); i.hasNext();)
        {
            EmitItem item = i.next();
            if (item.getSourceId().getIndex() == index)
            {
                i.remove();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(Object msgId)
    {
        KestrelSourceId sourceId = (KestrelSourceId) msgId;
        KestrelClientInfo info = this.clientInfoList.get(sourceId.getIndex());

        //if the transaction didn't exist, it just returns false. so this code works
        //even if client gets blacklisted, disconnects, and kestrel puts the item
        //back on the queue
        try
        {
            if (info.getClient() != null)
            {
                Set<Long> xids = Sets.newHashSet(sourceId.getId());
                info.getClient().confirm(this.queueName, xids);
            }
        }
        catch (TException e)
        {
            blacklist(info, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(Object msgId)
    {
        KestrelSourceId sourceId = (KestrelSourceId) msgId;
        KestrelClientInfo info = this.clientInfoList.get(sourceId.getIndex());

        // see not above about why this works with blacklisting strategy
        try
        {
            if (info.getClient() != null)
            {
                Set<Long> xids = Sets.newHashSet(sourceId.getId());
                info.getClient().abort(this.queueName, xids);
            }
        }
        catch (TException e)
        {
            blacklist(info, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(getOutputFields());
    }

    /**
     * @return the _queueName
     */
    protected String getQueueName()
    {
        return this.queueName;
    }

    /**
     * @param queueName the queueName to set
     */
    protected void setQueueName(String queueName)
    {
        this.queueName = queueName;
    }
}
