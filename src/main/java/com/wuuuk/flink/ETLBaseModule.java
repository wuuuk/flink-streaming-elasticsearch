package com.wuuuk.flink;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class ETLBaseModule {

    private static String baseMySQLConnect = "'connector' = 'mysql-cdc', " +
                                            "'hostname' = '%s', " +
                                            "'port' = '3306', " +
                                            "'username' = 'root', " +
                                            "'password' = 'root', " +
                                            "'database-name' = 'es', " +
                                            "'table-name' = '%s'";

    private static String baseElasticsearchConnect = "'connector.type' = 'elasticsearch', " +
                                                    "'connector.version' = '7', " +
                                                    "'connector.hosts' = 'http://" + "%s"+ ":9200', " +
                                                    "'connector.index' = '%s', " +
                                                    "'connector.document-type' = '%s', " +
                                                    "'update-mode'='upsert', " +
                                                    "'connector.key-delimiter' = '$', " +
                                                    "'connector.key-null-literal' = 'n/a', " +
                                                    "'connector.failure-handler' = 'retry-rejected', " +
                                                    "'connector.flush-on-checkpoint' = 'true', " +
                                                    "'connector.bulk-flush.max-actions' = '42', " +
                                                    "'connector.bulk-flush.max-size' = '42 mb', " +
                                                    "'connector.bulk-flush.interval' = '60000', " +
                                                    "'connector.connection-max-retry-timeout' = '300', " +
                                                    "'format.type' = 'json'";

    public static String getBaseMySQLConnect(String tableName){
        return getBaseMySQLConnect("mysql", tableName);
    }

    public static String getBaseMySQLConnect(String mysqlHost, String tableName){
        return String.format(baseMySQLConnect, mysqlHost, tableName);
    }

    public static String getBaseElasticsearchConnect(String indexName){
        return getBaseElasticsearchConnect("elasticsearch", indexName);
    }

    public static String getBaseElasticsearchConnect(String esHost, String indexName){
        return String.format(baseElasticsearchConnect, esHost, indexName + "_sink", indexName);
    }

    public static void setCheckPoint(StreamExecutionEnvironment env, String jobName) throws IOException {
        // checkpoint 用于恢复失败的job
        // RETAIN_ON_CANCELLATION: 作业取消时，保留作业的checkpoint, 需要手动清除该作业保留的 checkpoint
        // DELETE_ON_CANCELLATION: 作业取消时，删除作业的checkpoint, 失败时，会保留作业的checkpoint
//        String uri = "hdfs://namenode:40010/flink/checkpoints";
//        String uri = "file:///opt/flink/checkpoints/" + jobName;  // 文件地址
        String uri = "file:///Users/lintao/workspaces/docker_data/flink/checkpoints/" + jobName;
        StateBackend stateBackend = new RocksDBStateBackend(uri, true);  // 实例化StateBackend
        env.setStateBackend(stateBackend); // 设置StateBackend模式
        env.enableCheckpointing(1000 * 60 * 5);  // 指定的10000表示checkpoint间隔时间
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointTimeout(1000 * 60 * 60 * 24 * 3);
        config.setMinPauseBetweenCheckpoints(1000);
        config.setMaxConcurrentCheckpoints(3);  // checkpoint之间的最短间隔时间，最后一个检查点结束时间到下一个检查点开始时间之间的间隔
    }
}
