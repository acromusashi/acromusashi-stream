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

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.util.JsonValueExtractor;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

/**
 * KestrelからJSON形式のメッセージを取得し、下記の処理を行った上でBoltに流すSpout<br/>
 * <ol>
 * <li>インプット KestrelからJSON形式のメッセージを取得する</li>
 * <li>Spoutの処理 取得したJSON形式のメッセージからグルーピング情報を取得する</li>
 * <li>アウトプット グルーピング情報、JSON形式メッセージ</li>
 * </ol>
 * 
 * 本クラスが期待するキューデータの形式仕様は下記の通り。<br/>
 * 【グルーピング情報カテゴリ】、【グルーピング情報キー】の値はSpout生成時に指定すること。<br/>
 * <br/>-------------------------------------------------
 * <br/>
 * {<br/>
 * &nbsp;"【グルーピング情報カテゴリ】":<br/>
 * &nbsp;&nbsp;{<br/>
 * &nbsp;&nbsp;&nbsp;"【グルーピング情報キー】": "【グルーピング情報】",<br/>
 * &nbsp;&nbsp;&nbsp;・・・・<br/>
 * &nbsp;&nbsp;}<br/>
 * &nbsp;・・・・<br/>
 * }<br/>
 * -------------------------------------------------
 * <br/>
 * 
 * @author kimura
 */
public class KestrelJsonSpout extends KestrelSpout
{
    /** Kestrelにアクセスする際のサーバリストキー */
    public static final String          KESTREL_SERVERS       = "kestrel.servers";

    /** Kestrelにアクセスする際のキュー名称 */
    public static final String          KESTREL_QUEUE         = "kestrel.queue";

    /** Kestrelタイムアウト時間（単位：ミリ秒） */
    public static final String          KESTREL_TIMEOUT       = "kestrel.timeout";

    /** Kestrelの１回に取得する最大メッセージ数 */
    public static final String          KESTREL_BATCH_SIZE    = "kestrel.batch.size";

    /** Kestrelに接続失敗時に待機する時間 */
    public static final String          KESTREL_BLACKLISTTIME = "kestrel.blacklist.time";

    /** headerタグ名 */
    private static final String         HEADER_TAG            = "header";

    /** messageKeyタグ名 */
    private static final String         MESSAGEKEY_TAG        = "messageKey";

    /** serialVersionUID */
    private static final long           serialVersionUID      = -3331796053960250415L;

    /** logger */
    private static final Logger         logger                = LoggerFactory.getLogger(KestrelJsonSpout.class);

    /** 規制状態確認クラス */
    protected transient RestrictWatcher restrictWatcher;

    /** 規制ファイルパス */
    private String                      restrictFilePath;

    /**
     * コンストラクタ
     * 
     * @param hosts 取得対象キューホスト（【ホスト名:port】形式のリスト）
     * @param queueName 取得対象キュー名称
     * @param scheme 受信時の適用スキーム
     */
    public KestrelJsonSpout(List<String> hosts, String queueName, Scheme scheme)
    {
        super(hosts, queueName, new SchemeAsMultiScheme(scheme));

        if (StringUtils.isEmpty(queueName) == true)
        {
            throw new IllegalArgumentException("Must configure queueName");
        }

        if (scheme == null)
        {
            throw new IllegalArgumentException("Must configure scheme");
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes"})
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        super.open(conf, context, collector);

        Number timeout = (Number) conf.get(KESTREL_TIMEOUT);
        this.messageTimeoutMs = (int) TimeUnit.SECONDS.toMillis(timeout.intValue());

        this.restrictWatcher = new RestrictWatcher(this.restrictFilePath);

        // Spoutの番号に併せて取得対象となるQueue番号をQueue名称に設定
        int componentIndex = context.getThisTaskIndex();
        String baseQueueName = getQueueName();
        String queueName = baseQueueName + "_" + componentIndex;
        setQueueName(queueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Fields getOutputFields()
    {
        return new Fields(Arrays.asList(FieldName.MESSAGE_KEY, FieldName.MESSAGE_VALUE));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isRestricted()
    {
        return this.restrictWatcher.isRestrict();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected EmitItem generateEmitItem(List<Object> retItems, KestrelSourceId sourceId)
    {
        String groupingInfo = null;
        String jsonMessage = (String) retItems.get(0);

        try
        {
            // グルーピング情報を取得する
            groupingInfo = JsonValueExtractor.extractValue(jsonMessage, HEADER_TAG, MESSAGEKEY_TAG);
        }
        catch (JSONException jex)
        {
            String logFormat = "Received message is not json. : message={0}";
            logger.debug(MessageFormat.format(logFormat, jsonMessage), jex);
            return null;
        }

        // グルーピング情報をグルーピングキーに指定し、Boltにメッセージを送信する
        EmitItem generatedItem = new EmitItem(Arrays.asList((Object) groupingInfo, jsonMessage),
                sourceId);
        return generatedItem;
    }

    /**
     * @param restrictFilePath the restrictFilePath to set
     */
    public void setRestrictFilePath(String restrictFilePath)
    {
        this.restrictFilePath = restrictFilePath;
    }
}
