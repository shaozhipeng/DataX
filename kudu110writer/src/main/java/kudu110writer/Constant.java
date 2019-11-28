package com.alibaba.datax.plugin.writer.kudu110writer;

public class Constant {

    public static final int DEFAULT_BATCH_SIZE = 1024;

    public static final int DEFAULT_BATCH_BYTE_SIZE = 32 * 1024 * 1024;

    public static final int DEFAULT_KUDU_MUTATION_BUFFER_SPACE = 1000;

    public static final boolean DEFAULT_KUDU_ISUPSERT = false;

    public static final int DEFAULT_KUDU_WORKER_COUNT = 10;

    public static final int DEFAULT_OPERATION_TIMEOUT_MS = 10000;

    public static final int DEFAULT_ADMIN_OPERATION_TIMEOUT_MS = 30000;
}
