package com.alibaba.datax.plugin.writer.kudu110writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.datax.plugin.writer.kudu110writer.Constant.*;
import static com.alibaba.datax.plugin.writer.kudu110writer.KuduWriterErrorCode.*;

public class KuduWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        /**
         * @param mandatoryNumber 为了做到Reader、Writer任务数对等，这里要求Writer插件必须按照源端的切分数进行切分。否则框架报错！
         * @return List<Configuration>
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            LOG.info("Job初始化完成");
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String tableName;
        private int batchSize;
        private int batchByteSize;
        private int workerCount;
        private Configuration sliceConfig;
        private KuduClient kuduClient;
        private KuduSession kuduSession;
        private KuduTable kuduTable;
        private List<JSONObject> columnsList = new ArrayList<JSONObject>();
        private List<JSONObject> primaryKeyList = new ArrayList<>();

        private boolean isUpsert;

        @Override
        public void startWrite(RecordReceiver recordReceiver) {

            List<Record> writeBuffer = new ArrayList<>(this.batchSize);
            int bufferBytes = 0;

            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    // 简单验证字段列表和主键数据是否为空
                    if (columnsList.size() != record.getColumnNumber()) {
                        throw DataXException.asDataXException(ILLEGAL_VALUES_ERROR, ILLEGAL_VALUES_ERROR.getDescription() + "读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + columnsList.size());
                    }

                    boolean isDirtyRecord = false;
                    for (int i = 0; i < primaryKeyList.size() && !isDirtyRecord; i++) {
                        JSONObject col = primaryKeyList.get(i);
                        Column column = record.getColumn(col.getInteger("index"));
                        isDirtyRecord = (column.getRawData() == null);
                    }

                    if (isDirtyRecord) {
                        super.getTaskPluginCollector().collectDirtyRecord(record, "primarykey字段为空");
                        continue;
                    }

                    // 放到writeBuffer里面并计算字节总数
                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchFlush(writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }

                // 处理最后剩余的数据
                if (!writeBuffer.isEmpty()) {
                    doBatchFlush(writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }

            } catch (Exception e) {
                throw DataXException.asDataXException(KuduWriterErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
            }
        }

        private void doBatchFlush(List<Record> writeBuffer) {
            try {
                Iterator<Record> it = writeBuffer.iterator();
                while (it.hasNext()) {
                    // 这里只会有一种操作
                    Operation operation;
                    if (isUpsert) {
                        // 覆盖更新
                        operation = kuduTable.newUpsert();
                    } else {
                        // 增量更新
                        operation = kuduTable.newInsert();
                    }
                    convertRecordToRow(it.next(), operation.getRow());
                    kuduSession.apply(operation);
                }
                kuduSession.flush();
            } catch (KuduException e) {
                throw DataXException.asDataXException(KuduWriterErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        private void convertRecordToRow(Record record, PartialRow row) {
            for (int i = 0; i < columnsList.size(); i++) {

                JSONObject col = columnsList.get(i);
                String columnName = col.getString(Key.NAME);

                SupportKuduColumnType columnType = SupportKuduColumnType.valueOf(col.getString(Key.TYPE).toUpperCase());
                Column column = record.getColumn(col.getInteger(Key.INDEX));
                Object data = column.getRawData();
                if (data == null) {
                    row.setNull(columnName);
                    continue;
                }

                String rowData = data.toString();
                switch (columnType) {
                    case TINYINT:
                        row.addByte(columnName, Byte.valueOf(rowData));
                        break;
                    case SMALLINT:
                        row.addShort(columnName, Short.valueOf(rowData));
                        break;
                    case INT:
                        row.addInt(columnName, Integer.valueOf(rowData));
                        break;
                    case BIGINT:
                        row.addLong(columnName, column.asLong());
                        break;
                    case FLOAT:
                        row.addFloat(columnName, Float.valueOf(rowData));
                        break;
                    case DOUBLE:
                        row.addDouble(columnName, column.asDouble());
                        break;
                    case DECIMAL:
                        row.addDecimal(columnName, column.asBigDecimal());
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        // 如果源数据源编码不是UTF-8 可以addStringUtf8进行转换
                        row.addString(columnName, column.asString());
                        break;
                    case BOOLEAN:
                        row.addBoolean(columnName, column.asBoolean());
                        break;
                    case DATE:
                    case TIMESTAMP:
                        row.addTimestamp(columnName, new java.sql.Timestamp(column.asDate().getTime()));
                        break;
                    default:
                        throw DataXException
                                .asDataXException(
                                        ILLEGAL_VALUE,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                col.getString(Key.NAME),
                                                col.getString(Key.TYPE)));
                }
            }
        }

        /**
         * 获取配置与创建连接
         */
        @Override
        public void init() {
            // 获取与本task相关的配置
            this.sliceConfig = super.getPluginJobConf();

            // 判断表名是必须得参数
            tableName = sliceConfig.getNecessaryValue(Key.KEY_KUDU_TABLE, KUDU_ERROR_TABLE);

            // 设置主节点 host1:7051,host2:7051,host3:7051...
            String masterAddresses = sliceConfig.getNecessaryValue(Key.KEY_KUDU_MASTER, KUDU_ERROR_MASTER);

            // 设置批量大小，官方推荐最多不超过1024
            batchSize = sliceConfig.getInt(Key.KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE);

            // 设置批量字节大小
            batchByteSize = sliceConfig.getInt(Key.BATCH_BYTE_SIZE, DEFAULT_BATCH_BYTE_SIZE);

            // 设置缓冲大小，系统默认值是1000 往10000+去调整，否则速度太慢
            int mutationBufferSpace = sliceConfig.getInt(Key.KEY_KUDU_MUTATION_BUFFER_SPACE, DEFAULT_KUDU_MUTATION_BUFFER_SPACE);

            // 判断是覆盖更新还是增量更新，默认是增量更新；流水数据采用默认增量更新即可；有变化的数据采用updatetime做覆盖更新
            isUpsert = sliceConfig.getBool(Key.KEY_KUDU_ISUPSERT, DEFAULT_KUDU_ISUPSERT);

            // 获取指定的primary key，可以指定多个
            primaryKeyList = (List<JSONObject>) sliceConfig.get(Key.KEY_KUDU_PRIMARY, List.class);

            // 获取列名
            List<JSONObject> _columnList = (List<JSONObject>) sliceConfig.get(Key.KEY_KUDU_COLUMN, List.class);

            workerCount = sliceConfig.getInt(Key.KEY_KUDU_WORKER_COUNT, DEFAULT_KUDU_WORKER_COUNT);

            // 判断主键是否为空
            if (primaryKeyList == null || primaryKeyList.isEmpty()) {
                throw DataXException.asDataXException(KUDU_ERROR_PRIMARY_KEY, KUDU_ERROR_PRIMARY_KEY.getDescription());
            }

            for (JSONObject column : _columnList) {
                if (!column.containsKey(Key.INDEX)) {
                    throw DataXException.asDataXException(KUDU_ERROR_CONF_COLUMNS, KUDU_ERROR_CONF_COLUMNS.getDescription());
                } else if (!column.containsKey(Key.NAME)) {
                    throw DataXException.asDataXException(KUDU_ERROR_CONF_COLUMNS, KUDU_ERROR_CONF_COLUMNS.getDescription());
                } else if (!column.containsKey(Key.TYPE)) {
                    throw DataXException.asDataXException(KUDU_ERROR_CONF_COLUMNS, KUDU_ERROR_CONF_COLUMNS.getDescription());
                } else {
                    columnsList.add(column.getInteger(Key.INDEX), column);
                }
            }

            LOG.info("初始化KuduClient");

            // A synchronous and thread-safe client
            KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(masterAddresses)
                    // 此处如果有需要也可改为可配置
                    .defaultOperationTimeoutMs(DEFAULT_OPERATION_TIMEOUT_MS)
                    .defaultAdminOperationTimeoutMs(DEFAULT_ADMIN_OPERATION_TIMEOUT_MS);

            // If not provided, (2 * the number of available processors) is used.
            kuduClientBuilder.workerCount(workerCount);
            kuduClient = kuduClientBuilder.build();

            // AsyncKuduSession
            kuduSession = kuduClient.newSession();
            try {
                // 此处如果有需要也可改为可配置
                kuduSession.setExternalConsistencyMode(ExternalConsistencyMode.CLIENT_PROPAGATED);
            } catch (IllegalArgumentException ex) {
                throw DataXException.asDataXException(KUDU_ERROR_TABLE.EXTERNAL_CONSISTENCY_MODE, "kuduSession.setExternalConsistencyMode失败.");
            }

            // the writes will not be sent until the user calls {@link KuduSession#flush()}.
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            // private int mutationBufferMaxOps = 1000; mutationBufferSpace要大于等于batchSize
            kuduSession.setMutationBufferSpace(mutationBufferSpace);
            try {
                kuduTable = kuduClient.openTable(tableName);
            } catch (KuduException e) {
                throw DataXException.asDataXException(KUDU_ERROR_TABLE.ILLEGAL_VALUE, "打开Kudu表失败:" + tableName);
            }
        }

        @Override
        public void destroy() {
            try {
                if (null != kuduSession && !kuduSession.isClosed()) {
                    kuduSession.close();
                }
                if (null != kuduClient) {
                    kuduClient.close();
                }
                kuduClient = null;
                kuduSession = null;
            } catch (Exception e) {

            }
        }
    }
}
