package com.wuuuk.flink.mysql;

import com.wuuuk.flink.ETLBaseModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserPricesView {

    public static void main(String[] args) throws Exception{
        executeJob();
    }

    public static void executeJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ETLBaseModule.setCheckPoint(env, "user_prices");
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        String userSource = ETLBaseModule.getBaseMySQLConnect("users");
        String orderSource = ETLBaseModule.getBaseMySQLConnect("orders");
        String orderItemSource = ETLBaseModule.getBaseMySQLConnect("order_items");
        String userPriceSink = ETLBaseModule.getBaseElasticsearchConnect("user_prices");

        String sourceDDL1 = "CREATE TABLE users (" +
                            "id STRING, " +
                            "name STRING, " +
                            "age INT, " +
                            "ctime TIMESTAMP(3), " +
                            "utime TIMESTAMP(3), " +
                            "PRIMARY KEY (id) NOT ENFORCED " +
                            ") WITH (" + userSource + ")";

        String sourceDDL2 = "CREATE TABLE orders (" +
                            "id STRING, " +
                            "amount BIGINT, " +
                            "user_id STRING, " +
                            "ctime TIMESTAMP(3), " +
                            "utime TIMESTAMP(3), " +
                            "PRIMARY KEY (id) NOT ENFORCED " +
                            ") WITH (" + orderSource + ")";

        String sourceDDL3 = "CREATE TABLE order_items (" +
                            "id String, " +
                            "order_id String, " +
                            "product_id String," +
                            "price BIGINT, " +
                            "discount BIGINT, " +
                            "quantity DECIMAL, " +
                            "ctime TIMESTAMP(3), " +
                            "utime TIMESTAMP(3), " +
                            "PRIMARY KEY (id) NOT ENFORCED" +
                            ") WITH (" + orderItemSource + ")";

        String sinkDDL = "CREATE TABLE user_prices_sink (" +
                         "id STRING, " +
                         "price1 BIGINT, " +
                         "PRIMARY KEY (id) NOT ENFORCED " +
                         ") WITH (" + userPriceSink + ")";

        String handlerSQL = "INSERT INTO user_prices_sink SELECT " +
                                "user_id id, " +
                                "SUM(price) price " +
                            "FROM order_items " +
                            "JOIN orders " +
                            "ON " +
                                "order_id = orders.id " +
                            "GROUP BY user_id, mod(hash_code(FLOOR(RAND(1)*1000)), 256)" ;

//        System.out.println(sourceDDL1);
//        System.out.println(sourceDDL2);
//        System.out.println(sourceDDL3);
//        System.out.println(sinkDDL);
//        System.out.println(handlerSQL);
        streamTableEnvironment.executeSql(sourceDDL1);
        streamTableEnvironment.executeSql(sourceDDL2);
        streamTableEnvironment.executeSql(sourceDDL3);
        streamTableEnvironment.executeSql(sinkDDL);
        streamTableEnvironment.executeSql(handlerSQL);
    }
}
