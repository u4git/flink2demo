package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is aggregate4streamv1...");

        // 创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

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

        // 会话窗口（只能按时间）
        wordGroups.window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(30))).aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("createAccumulator()");
                return 0;
            }

            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("add(): value=" + value + ", accumulator=" + accumulator);
                return accumulator + value.f1;
            }

            @Override
            public String getResult(Integer accumulator) {
                System.out.println("getResult(): accumulator=" + accumulator);
                return "accumulator: " + accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("merge(): a=" + a + ", b=" + b);
                return a + b;
            }
        }).print("aggregate");

        // 执行

        env.execute("aggregate4streamv1");

        System.out.println("This is aggregate4streamv1...done.");
    }
}