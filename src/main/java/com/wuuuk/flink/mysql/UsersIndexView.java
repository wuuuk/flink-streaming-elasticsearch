package com.wuuuk.flink.mysql;

import com.wuuuk.flink.ETLBaseModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.io.IOException;

public class UsersIndexView {

    public static void main(String[] args) throws Exception {
        tableUsers();
    }

    private static void tableUsers() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ETLBaseModule.setCheckPoint(env, "users");
        StreamTableEnvironment streamEnvironment = StreamTableEnvironment.create(env);

        String usersSource = ETLBaseModule.getBaseMySQLConnect("users");
        String usersSink = ETLBaseModule.getBaseElasticsearchConnect("products_sale");

        String userSource = "CREATE TABLE users (" +
                "id STRING, " +
                "name STRING, " +
                "age INT, " +
                "ctime TIMESTAMP(3), " +
                "utime TIMESTAMP(3), " +
                "PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH (" + usersSource + ")";

        String userSink = "CREATE TABLE users_sink (" +
                "id STRING, " +
                "name STRING, " +
                "age INT, " +
                "ctime TIMESTAMP(3), " +
                "utime TIMESTAMP(3), " +
                "PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH (" + usersSink + ")";

        String handlerSQL = "INSERT INTO users_sink SELECT id, name, age, ctime, utime FROM users";

        streamEnvironment.executeSql(userSource);
        streamEnvironment.executeSql(userSink);
        streamEnvironment.executeSql(handlerSQL);
    }
}
