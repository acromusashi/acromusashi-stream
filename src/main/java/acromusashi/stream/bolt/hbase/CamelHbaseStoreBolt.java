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
package acromusashi.stream.bolt.hbase;

import java.util.List;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.hbase.model.HBaseCell;
import org.apache.camel.component.hbase.model.HBaseData;
import org.apache.camel.component.hbase.model.HBaseRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.MessageBolt;
import acromusashi.stream.camel.CamelInitializer;
import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.exception.ConvertFailException;
import acromusashi.stream.exception.InitFailException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

/**
 * Camel-HBase Componentを利用して、受信したMessageをHBaseに保存するBolt。<br/>
 * 指定したテーブル(endpointUriTo参照)に対して下記の法則でDataを生成して投入を行う。<br/>
 * <br/>
 * <li>rowId = Message:HeaderのTimeStamp + "_" + Message:HeaderのSource</li>
 * <li>Message:Bodyの内容をリスト化し、変数cellDefineListの値に併せてFamily/Quantifierを設定</li>
 * 
 * @author otoda
 */
public class CamelHbaseStoreBolt extends MessageBolt
{
    /** serialVersionUID */
    private static final long          serialVersionUID = -668373233969623288L;

    /** logger */
    private static final Logger        logger           = LoggerFactory.getLogger(CamelHbaseStoreBolt.class);

    /** ApplicationContextのファイルパス */
    private String                     contextUri;

    /** デフォルトのEndPoint */
    private String                     defaultEndPoint  = "direct:hbase";

    /** HBase上の投入先を示すCell定義 */
    private List<CellDefine>           cellDefineList;

    /** Camelにオブジェクトを送信するクラス */
    private transient ProducerTemplate producerTemplate;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public CamelHbaseStoreBolt()
    {}

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);

        // Camelを初期化する
        try
        {
            this.producerTemplate = CamelInitializer.generateTemplete(this.contextUri);
        }
        catch (Exception ex)
        {
            logger.error("Failure to get ProducerTemplate.", ex);
            throw new InitFailException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMessage(StreamMessage message) throws ConvertFailException
    {
        Map<String, Object> values = this.converter.toMap(message);

        // rowidを設定する。
        HBaseData data = new HBaseData();
        HBaseRow row = new HBaseRow();
        row.setId(values.get("timestamp").toString() + "_" + values.get("source").toString());

        List<String> bodyList = (List<String>) values.get("body");

        for (int i = 0; i < bodyList.size(); i++)
        {
            CellDefine cellDefine = this.cellDefineList.get(i);

            HBaseCell registCell = new HBaseCell();
            registCell.setFamily(cellDefine.family);
            registCell.setQualifier(cellDefine.qualifier);
            registCell.setValue(bodyList.get(i));
            row.getCells().add(registCell);
        }

        data.getRows().add(row);

        insert(data);
    }

    /**
     * endpointUriを指定して、DBにinsertを行う。
     * 
     * @param values
     *            PreparedStatementに設定する値
     */
    protected void insert(HBaseData values)
    {
        this.producerTemplate.sendBody(this.defaultEndPoint, values);
    }

    /**
     * ApplicationContextUriを指定する。 このメソッドは
     * {@link #prepare(Map, TopologyContext, OutputCollector)}が呼び出されるより前に呼び出すこと。
     * したがって、Topologyをsubmitするより前に呼び出すこと。 ApplicationContextUriに指定したファイルは
     * {@link #prepare(Map, TopologyContext, OutputCollector)}で読み込む。
     * 
     * @param applicationContextUri
     *            ApplicationContextUri
     */
    public void setApplicationContextUri(String applicationContextUri)
    {
        this.contextUri = applicationContextUri;
    }

    /**
     * @param cellDefineList the cellDefineList to set
     */
    public void setCellDefineList(List<CellDefine> cellDefineList)
    {
        this.cellDefineList = cellDefineList;
    }

}
