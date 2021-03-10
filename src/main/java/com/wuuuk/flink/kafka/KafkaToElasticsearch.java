package com.wuuuk.flink.kafka;

import com.wuuuk.flink.ETLBaseModule;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

import com.alibaba.fastjson.JSONObject;


public class KafkaToElasticsearch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ETLBaseModule.setCheckPoint(env, "kafka_source");

        // kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "group");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("sync.flink_users", new SimpleStringSchema(), properties);
//        flinkKafkaConsumer.setStartFromEarliest(); // 从最早开始
        flinkKafkaConsumer.setStartFromGroupOffsets(); // 默认方法
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false); // 如果启用了checkpoint，当checkpoint完成时，kafka将提交的offset存储在checkpoint状态中。会报错
        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer);

        DataStream<JSONObject> dataStream = dataStreamSource
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String result) throws Exception {
                        return JSONObject.parseObject(result);
                    }
                })
                // keyBy, 分区，用tuple2
                .keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JSONObject result) throws Exception {
                        return new Tuple2<String, String>(result.getString("resource_id"), result.getString("metric"));
                    }
                })
                // window, 时间设定5分钟窗口
                .timeWindow(Time.seconds(60 * 5))
                //  窗口内计算
                .reduce(new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject t0, JSONObject t1) throws Exception {
                        if (t1.getInteger("value") - t0.getInteger("value") == 0) {
                            System.out.println("t1:" + t1.getInteger("value"));
                            System.out.println("t2:" + t1.getInteger("value"));
                        }
                        t0.put("value", t1.getInteger("value") - t0.getInteger("value"));
                        return t0;
                    }
                });

        // es sink
        List<HttpHost> esHttpHost = new ArrayList<>();
        esHttpHost.add(new HttpHost("elasticsearch", 9200, "http"));
        ElasticsearchSink.Builder<JSONObject> esBuilder = new ElasticsearchSink.Builder<>(esHttpHost, new ElasticsearchSinkFunction<JSONObject>() {
            public IndexRequest createIndexRequest(JSONObject result){
                result.put("time", System.currentTimeMillis() / 1000);
                return Requests.indexRequest()
                        .index("test_index")
                        .type("test_type")
                        .source(result);
            }
            @Override
            public void process(JSONObject jsonObject, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(createIndexRequest(jsonObject));
            }
        });

        esBuilder.setBulkFlushMaxActions(1);  // 刷新前缓冲的最大动作量
        dataStream.addSink(esBuilder.build()); // source add sink
        env.execute("kafka source");
    }
}