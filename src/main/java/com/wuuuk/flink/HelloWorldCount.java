package com.wuuuk.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class HelloWorldCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int countDownSeconds = -1;
        if (args.length == 1) {
            countDownSeconds = Integer.parseInt(args[0]);
        }
        DataStream<String> streamSource = env.addSource(new WordCountCountDownSource(countDownSeconds)).name("Count Down (" + countDownSeconds + ")");
        DataStream<WordWithCount> dataStream = streamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
                    public void flatMap(String value, Collector<HelloWorldCount.WordWithCount> out) throws Exception {
                        String[] words = value.split(",");
                        for (String w : words) {
                            HelloWorldCount.WordWithCount wc = new HelloWorldCount.WordWithCount(w, 1);
                            out.collect(wc);
                        }
                    }
                }).keyBy(new String[]{"word"}).reduce(new ReduceFunction<WordWithCount>() {
                    public HelloWorldCount.WordWithCount reduce(HelloWorldCount.WordWithCount value1, HelloWorldCount.WordWithCount value2)throws Exception {
                        return new HelloWorldCount.WordWithCount(value1.word, value1.count + value2.count);
                    }
                });
        dataStream.print().name("Print-Only Sink");
        env.execute(HelloWorldCount.class.getSimpleName());
    }

    public static class WordWithCount {
        private String word;
        private int count;

        public WordWithCount() {}

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        WordWithCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
        public String toString() {
            return "word:" + this.word + ", count:" + this.count;
        }
    }

    public static class WordCountCountDownSource implements SourceFunction<String> {
        private long countDown;

        public WordCountCountDownSource(long countDown) {
            if (countDown > 0) {
                this.countDown = countDown;
            } else {
                this.countDown = Long.MAX_VALUE;
            }
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (countDown > 0) {
                ctx.collect("Hello World");
                Thread.sleep(1000L);
                countDown--;
            }
        }

        @Override
        public void cancel() {
        }
    }
}
