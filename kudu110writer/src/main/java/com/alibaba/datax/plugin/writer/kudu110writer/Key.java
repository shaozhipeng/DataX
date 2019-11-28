package com.alibaba.datax.plugin.writer.kudu110writer;

public class Key {
    public static final String KEY_KUDU_TABLE = "table";

    public static final String KEY_KUDU_MASTER = "master";

    // 默认值：1024
    public static final String KEY_BATCH_SIZE = "batchSize";

    public static final String KEY_KUDU_WORKER_COUNT = "workerCount";

    public static final String KEY_KUDU_MUTATION_BUFFER_SPACE = "mutationBufferSpace";

    public static final String KEY_KUDU_ISUPSERT = "isUpsert";

    public static final String KEY_KUDU_PRIMARY = "primaryKey";

    public static final String KEY_KUDU_COLUMN = "column";

    public static final String TYPE = "type";

    public static final String NAME = "name";

    public static final String INDEX = "index";

    // 默认值：32m
    public final static String BATCH_BYTE_SIZE = "batchByteSize";
}
