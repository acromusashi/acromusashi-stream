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

import java.text.MessageFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.RotatingMap;

/**
 * DRPCリクエストを受信してTridentの処理を開始するSpout用のEmitterクラス
 * 
 * @author sotaro
 */
public abstract class DrpcTridentEmitter implements Emitter<Object>
{
    /** ロガー */
    private static Logger                                      logger      = LoggerFactory.getLogger(DrpcTridentEmitter.class);

    /** ローテートマップのサイズ。タイムアウト秒経過毎に現状の内容がシフトされるため、サイズは3にして最後のマップにシフトしたらタイムアウトして扱う */
    private static final int                                   ROTATE_SIZE = 3;

    /** DRPCで受信対象とする機能名 */
    protected String                                           function;

    /** SpoutEmitterの初期化済みフラグ */
    protected boolean                                          prepared;

    /** タイムアウト検知用のマップ */
    protected RotatingMap<TransactionAttempt, DrpcRequestInfo> idsMap;

    /** 最後にタイムアウト検知用のマップをローテートした時間 */
    protected long                                             lastRotate;

    /** タイムアウト検知用のマップを切り替えるタイミング */
    protected long                                             rotateTime;

    /** StormConfig */
    @SuppressWarnings("rawtypes")
    protected Map                                              stormConf;

    /** TopologyContext */
    protected TopologyContext                                  context;

    /** DRPCリクエストを取得するヘルパークラス */
    protected transient DrpcFetchHelper                        fetchHelper;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public DrpcTridentEmitter()
    {}

    /**
     * 設定値、Context、DRPC機能名称を指定して初期化を行う。
     * 
     * @param conf 設定値
     * @param context Context
     * @param function DRPC機能名称
     */
    @SuppressWarnings("rawtypes")
    public void initialize(Map conf, TopologyContext context, String function)
    {
        this.context = context;
        this.stormConf = conf;
        this.function = function;
        this.idsMap = new RotatingMap<TransactionAttempt, DrpcRequestInfo>(ROTATE_SIZE);
        this.rotateTime = TimeUnit.SECONDS.toMillis(((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue());
        this.lastRotate = getCurrentTime();
    }

    /**
     * SpoutEmitterの初期化処理を行う。
     * 
     * @param conf Storm設定オブジェクト
     * @param context Topologyコンテキスト
     */
    @SuppressWarnings("rawtypes")
    protected abstract void prepare(Map conf, TopologyContext context);

    /**
     * {@inheritDoc}
     */
    @Override
    public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector)
    {
        long now = getCurrentTime();
        if (now - this.lastRotate > this.rotateTime)
        {
            Map<TransactionAttempt, DrpcRequestInfo> failed = this.idsMap.rotate();
            for (Entry<TransactionAttempt, DrpcRequestInfo> entry : failed.entrySet())
            {
                fail(entry.getKey(), entry.getValue());
            }
            this.lastRotate = now;
        }

        if (this.idsMap.containsKey(tx) == true)
        {
            DrpcRequestInfo requestInfo = (DrpcRequestInfo) this.idsMap.remove(tx);
            fail(tx, requestInfo);
        }

        // 初期化未実施の場合初期化処理を行う。
        if (this.prepared == false)
        {
            prepare(this.stormConf, this.context);
            this.fetchHelper = createFetchHelper();
            this.fetchHelper.initialize(stormConf, this.function);
            prepared = true;
        }

        // DRPCリクエストを取得する。リクエストが存在しない場合、何も処理は行わない。
        DrpcRequestInfo requestInfo = null;
        try
        {
            requestInfo = this.fetchHelper.fetch();
        }
        catch (TException ex)
        {
            logger.warn("DRPC fetch failed.", ex);
        }

        if (requestInfo != null)
        {
            if (logger.isDebugEnabled() == true)
            {
                String logFormat = "Request info get succeeded. TransactionAttempt={0}, DrpcRequestInfo={1}";
                logger.debug(MessageFormat.format(logFormat, tx, requestInfo));
            }

            emitTuples(requestInfo.getFuncArgs(), collector);
            idsMap.put(tx, requestInfo);
        }
    }

    /**
     * DRPCリクエストの引数を基にTupleの取得/Emitを行う。1トランザクション分のTupleの処理を行う。
     * 
     * @param funcArgs 引数
     * @param collector TridentCollector
     */
    protected abstract void emitTuples(String funcArgs, TridentCollector collector);

    /**
     * {@inheritDoc}
     */
    @Override
    public void success(TransactionAttempt tx)
    {
        DrpcRequestInfo requestInfo = (DrpcRequestInfo) this.idsMap.remove(tx);
        if (requestInfo != null)
        {
            ack(tx, requestInfo);
        }
    }

    /**
     * トランザクションが成功した際に呼び出される処理
     * 
     * @param tx トランザクション管理情報
     * @param requestInfo DRPCリクエスト情報
     */
    protected void ack(TransactionAttempt tx, DrpcRequestInfo requestInfo)
    {
        if (logger.isDebugEnabled() == true)
        {
            String logFormat = "Transaction succeeded. TransactionAttempt={0}, DrpcRequestInfo={1}";
            logger.debug(MessageFormat.format(logFormat, tx, requestInfo));
        }

        try
        {
            this.fetchHelper.ack(requestInfo.getRequestId(), "Succeeded");
        }
        catch (TException ex)
        {
            String logFormat = "Success notify failed. TransactionAttempt={0}, DrpcRequestInfo={1}";
            logger.warn(MessageFormat.format(logFormat, tx, requestInfo));
        }
    }

    /**
     * トランザクションが失敗した際に呼び出される処理
     * 
     * @param tx トランザクション管理情報
     * @param requestInfo DRPCリクエスト情報
     */
    protected void fail(TransactionAttempt tx, DrpcRequestInfo requestInfo)
    {
        if (logger.isDebugEnabled() == true)
        {
            String logFormat = "Transaction failed. TransactionAttempt={0}, DrpcRequestInfo={1}";
            logger.debug(MessageFormat.format(logFormat, tx, requestInfo));
        }

        try
        {
            this.fetchHelper.fail(requestInfo.getRequestId());
        }
        catch (TException ex)
        {
            String logFormat = "Fail notify failed. TransactionAttempt={0}, DrpcRequestInfo={1}";
            logger.warn(MessageFormat.format(logFormat, tx, requestInfo));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        // Do nothing.
    }

    /**
     * 現在の時刻値を取得する。
     * 
     * @return 現在の時刻値
     */
    protected long getCurrentTime()
    {
        return System.currentTimeMillis();
    }

    /**
     * DRPCヘルパーを生成する。
     * 
     * @return DRPCヘルパー
     */
    protected DrpcFetchHelper createFetchHelper()
    {
        return new DrpcFetchHelper();
    }
}
