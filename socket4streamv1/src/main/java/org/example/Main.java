package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is socket4streamv1...");

        // 创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 读取数据

        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        lines.print("lines");

        // 分词
        
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordPairs = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                
                String[] words = value.split(" ");

                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        
        // 分组

        KeyedStream<Tuple2<String, Integer>, String> wordGroups = wordPairs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 统计

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordGroups.sum(1);

        // 打印结果

        wordCount.print();

        // 执行

        env.execute("socket4streamv1");

        System.out.println("This is socket4streamv1...done.");
    }
}