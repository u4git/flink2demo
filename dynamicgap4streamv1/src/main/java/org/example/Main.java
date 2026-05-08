package org.example;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is dynamicgap4streamv1...");

        // 创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 读取数据

        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        lines.print("lines");

        // 构建对象

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> wordPairs = lines.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

                String[] fields = value.split(",");

                out.collect(Tuple3.of(fields[0], Integer.parseInt(fields[1]), Integer.parseInt(fields[2])));
            }
        });

        // 分组

        KeyedStream<Tuple3<String, Integer, Integer>, String> wordGroups = wordPairs.keyBy(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 按时间，滚动窗口，process 方法
        wordGroups.window(DynamicProcessingTimeSessionWindows.withDynamicGap(
                        new SessionWindowTimeGapExtractor<Tuple3<String, Integer, Integer>>() {
                            @Override
                            public long extract(Tuple3<String, Integer, Integer> element) {
                                return element.f2 * 1000;
                            }
                        })
                )
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Integer>, Tuple1<String>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple3<String, Integer, Integer>, Tuple1<String>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<Tuple1<String>> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss,SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss, SSS");

                        long count = elements.spliterator().estimateSize();

                        int sum = 0;

                        for (Tuple3<String, Integer, Integer> element : elements) {
                            sum += element.f1;
                        }

                        out.collect(Tuple1.of(String.format("Key: %s, Window: %s - %s, Count: %d, Sum: %d, Elements: %s", s, windowStart, windowEnd, count, sum, elements.toString())));
                    }
                }).print("keyed, time, tumbling");

        // 执行

        env.execute("dynamicgap4streamv1");

        System.out.println("This is dynamicgap4streamv1...done.");
    }
}