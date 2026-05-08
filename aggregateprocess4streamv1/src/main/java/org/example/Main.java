package org.example;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is process4streamv1...");

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

        // 按时间，滚动窗口，process 方法
        wordGroups.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))).aggregate(
                new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        System.out.println("createAccumulator()");
                        return null;
                    }

                    @Override
                    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                        System.out.println("add(), value=" + value + ", accumulator=" + accumulator);
                        return Tuple2.of(value.f0, value.f1 + (accumulator == null ? 0 : accumulator.f1));
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        System.out.println("getResult(), accumulator=" + accumulator);
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        System.out.println("merge(), a=" + a + ", b=" + b);
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                },
                new ProcessWindowFunction<Tuple2<String, Integer>, Tuple1<String>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, Tuple1<String>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple1<String>> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss,SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss, SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect(Tuple1.of(String.format("Key: %s, Window: %s - %s, Count: %d, Elements: %s", s, windowStart, windowEnd, count, elements.toString())));
                    }
                }
        ).print("keyed, time, tumbling");

        // 执行

        env.execute("process4streamv1");

        System.out.println("This is process4streamv1...done.");
    }
}