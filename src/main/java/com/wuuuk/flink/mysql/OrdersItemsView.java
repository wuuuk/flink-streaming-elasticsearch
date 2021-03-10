package com.wuuuk.flink.mysql;

import com.wuuuk.flink.ETLBaseModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;


public class OrdersItemsView {

    public static void main(String[] args) throws Exception {
        tableOrderItems();
    }

    private static void tableOrderItems() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        ETLBaseModule.setCheckPoint(env, "order_items");
        StreamTableEnvironment streamEnvironment = StreamTableEnvironment.create(env);
        String schema = "id STRING, product_id STRING, order_id STRING, price BIGINT, ctime TIMESTAMP(3), utime TIMESTAMP(3), PRIMARY KEY (id) NOT ENFORCED";
        String source_table = "order_items"; // 数据源表名
        String sink_table = source_table + "_sink";  //sink表名
        String base_sql = "CREATE TABLE %s (%s) WITH (" +
                "'connector' = 'mysql-cdc', " +
                "'hostname' = 'mysql', " +
                "'port' = '3306', " +
                "'username' = 'root', " +
                "'password' = 'root', " +
                "'database-name' = 'es'," +
                "'table-name' = '%s')";

        // source table
        String source_ddl = String.format(base_sql, source_table, schema, source_table);
        streamEnvironment.executeSql(source_ddl);

        String base_sink_ddl = "CREATE TABLE %s (%s) WITH ('connector.type' = 'elasticsearch','connector.version' = '7'," +
                "'connector.hosts' = 'http://elasticsearch:9200'," +
                "'connector.index' = 'order_items_view'," +
                "'connector.document-type' = 'order_items'," +
                "'update-mode'='upsert'," +
                "'connector.key-delimiter' = '$'," +
                "'connector.key-null-literal' = 'n/a'," +
                "'connector.failure-handler' = 'retry-rejected'," +
                "'connector.flush-on-checkpoint' = 'true'," +
                "'connector.bulk-flush.max-actions' = '42'," +
                "'connector.bulk-flush.max-size' = '42 mb'," +
                "'connector.bulk-flush.interval' = '60000'," +
                "'connector.connection-max-retry-timeout' = '300'," +
                "'format.type' = 'json'" +
                ")";

        // sink table
        String sink_ddl = String.format(base_sink_ddl, sink_table, schema);
        streamEnvironment.executeSql(sink_ddl);
        // insert sql command
        String insert_sql = "INSERT INTO order_items_sink SELECT id, product_id, order_id, price, ctime, utime FROM order_items";
        streamEnvironment.executeSql(insert_sql);
    }

    }
