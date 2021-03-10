package com.wuuuk.flink.mysql;

import com.wuuuk.flink.ETLBaseModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class ProductSalesView {

    public static void main(String[] args) throws Exception {
        executeOne();
    }

    private static void executeOne() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ETLBaseModule.setCheckPoint(env, "product_sales");
        StreamTableEnvironment streamEnvironment = StreamTableEnvironment.create(env);

        String ordersSource = ETLBaseModule.getBaseMySQLConnect("orders");
        String orderItemsSource = ETLBaseModule.getBaseMySQLConnect("order_items");
        String productsSource = ETLBaseModule.getBaseMySQLConnect("products");
        String productSalesSink = ETLBaseModule.getBaseElasticsearchConnect("products_sale");

        String sourceDDL1 = "CREATE TABLE orders(" +
                            "id String, " +
                            "amount BIGINT, " +
                            "ctime TIMESTAMP(3), " +
                            "utime TIMESTAMP(3)," +
                            "PRIMARY KEY (id) NOT ENFORCED" +
                            ") WITH (" + ordersSource + ")";

        String sourceDDL2 = "CREATE TABLE order_items(" +
                "id String, " +
                "order_id String, " +
                "product_id String," +
                "price BIGINT," +
                "discount BIGINT, " +
                "quantity DECIMAL," +
                "ctime TIMESTAMP(3), " +
                "utime TIMESTAMP(3), " +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" + orderItemsSource + ")";

        String sourceDDL3 = "CREATE TABLE products(" +
                "id STRING, " +
                "title STRING, " +
                "price BIGINT, " +
                "ctime TIMESTAMP(3), " +
                "utime TIMESTAMP(3)," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" + productsSource + ")";

        String sinkDDL = "CREATE TABLE product_sales_sink(" +
                         "id String, " +
                         "quantity BIGINT, " +
                         "PRIMARY KEY (id) NOT ENFORCED " +
                         ") WITH (" + productSalesSink + ")";

        String handlerSQL = "INSERT INTO product_sales_sink SELECT " +
                                "product_id id, " +
                                "SUM(quantity1) quantity  " +
                            "FROM (" +
                                "SELECT " +
                                    "order_items.product_id product_id, " +
                                    "COUNT(*) quantity1 " +
                                "FROM order_items " +
                                "JOIN orders " +
                                "ON " +
                                    "order_items.order_id = order_id " +
                                "GROUP BY order_items.product_id, mod(hash_code(FLOOR(RAND(1)*1000)), 256)" +
                                ")" +
                            "GROUP BY product_id";

        streamEnvironment.executeSql(sourceDDL1);
        streamEnvironment.executeSql(sourceDDL2);
        streamEnvironment.executeSql(sourceDDL3);
        streamEnvironment.executeSql(sinkDDL);
        streamEnvironment.executeSql(handlerSQL);
    }

}
