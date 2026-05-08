package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is reduce4streamv1...");

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

        // 按时间，滚动窗口
        wordGroups.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).print("keyed, time, tumbling");

        // 执行

        env.execute("reduce4streamv1");

        System.out.println("This is reduce4streamv1...done.");
    }
}