package com.wuuuk.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.io.IOException;

public class KafkaToES {

    public static void main(String[] args) throws Exception {
        tableTest();
    }
    private static void tableTest() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ETLBaseModule.setCheckPoint(env, "test");
        StreamTableEnvironment streamEnvironment = StreamTableEnvironment.create(env);

        String usersSink = ETLBaseModule.getBaseElasticsearchConnect("192.168.0.99","test");
        String sourceDDL1 = "CREATE TABLE test (" +
                "resource STRING, " +
                "resource_id STRING, " +
                "metric STRING, " +
                "`value` BIGINT, " +
                "parent_id STRING " +
                ") WITH ( " +
                "'connector' = 'kafka', " +
                "'topic' = 'aweme.sync.flink_users', " +
                "'properties.bootstrap.servers' = 'localhost:9092', " +
                "'properties.group.id' = 'group', " +
                "'scan.startup.mode' = 'group-offsets', " +
                "'format' = 'json', " +
                "'json.ignore-parse-errors' = 'true' " +
                ")";

        String userSink = "CREATE TABLE test_sink (" +
                "resource STRING, " +
                "resource_id STRING, " +
                "metric STRING, " +
                "`value` BIGINT, " +
                "parent_id STRING, " +
                "PRIMARY KEY (resource_id) NOT ENFORCED " +
                ") WITH (" + usersSink + ")";

        String handlerSQL = "INSERT INTO test_sink SELECT resource, resource_id, metric, `value`, parent_id FROM test";

        streamEnvironment.executeSql(sourceDDL1);
        streamEnvironment.executeSql(userSink);
        streamEnvironment.executeSql(handlerSQL);
    }
}
