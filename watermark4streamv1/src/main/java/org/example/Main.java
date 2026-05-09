package org.example;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is watermark4streamv1...");

        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        lines.print("lines");

        // 提取字段
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> wordPairs = lines.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Long>>() {

            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

                String[] words = value.split(",");

                out.collect(Tuple3.of(words[0], Integer.parseInt(words[1]), Long.valueOf(words[2])));
            }
        });

        /*
        有序流的 watermark
        - 没有等待时间
         */

        // 设置 watermark 策略
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> monotonousWatermark = wordPairs.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                return element.f2 * 1000;
            }
        }));

        // 分组
        KeyedStream<Tuple3<String, Integer, Long>, String> monotonousGroups = monotonousWatermark.keyBy(new KeySelector<Tuple3<String, Integer, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
                return value.f0;
            }
        });

        // 按窗口统计
        SingleOutputStreamOperator<Tuple1<String>> monotonousProcess = monotonousGroups.window(TumblingEventTimeWindows.of(Duration.ofSeconds(10))).process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple1<String>, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple1<String>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple1<String>> out) throws Exception {
                String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss,SSS");
                String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss, SSS");

                long currentWatermark = context.currentWatermark();

                System.out.println(String.format("MonotonousTimestamps, Window: %s - %s, Current Watermark: %s", windowStart, windowEnd, DateFormatUtils.format(currentWatermark, "yyyy-MM-dd HH:mm:ss, SSS")));

                long count = elements.spliterator().estimateSize();

                int sum = 0;

                for (Tuple3<String, Integer, Long> element : elements) {
                    sum += element.f1;
                }

                out.collect(Tuple1.of(String.format("Key: %s, Window: %s - %s, Count: %d, Sum: %s, Elements: %s", s, windowStart, windowEnd, count, sum, elements.toString())));
            }
        });

        // 打印结果
        monotonousProcess.print("MonotonousTimestamps");

        /*
        乱序流的 watermark
        - 有等待时间
         */

        // 设置 watermark 策略
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> outOfOrderWatermark = wordPairs.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                return element.f2 * 1000;
            }
        }));

        // 分组
        KeyedStream<Tuple3<String, Integer, Long>, String> outOfOrderGroups = outOfOrderWatermark.keyBy(new KeySelector<Tuple3<String, Integer, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Long> value) throws Exception {
                return value.f0;
            }
        });

        // 按窗口统计
        SingleOutputStreamOperator<Tuple1<String>> outOfOrderProcess = outOfOrderGroups.window(TumblingEventTimeWindows.of(Duration.ofSeconds(10))).process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple1<String>, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple1<String>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple1<String>> out) throws Exception {
                String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss,SSS");
                String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss, SSS");

                long currentWatermark = context.currentWatermark();

                System.out.println(String.format("BoundedOutOfOrderness, Window: %s - %s, Current Watermark: %s", windowStart, windowEnd, DateFormatUtils.format(currentWatermark, "yyyy-MM-dd HH:mm:ss, SSS")));

                long count = elements.spliterator().estimateSize();

                int sum = 0;

                for (Tuple3<String, Integer, Long> element : elements) {
                    sum += element.f1;
                }

                out.collect(Tuple1.of(String.format("Key: %s, Window: %s - %s, Count: %d, Sum: %s, Elements: %s", s, windowStart, windowEnd, count, sum, elements.toString())));
            }
        });

        // 打印结果
        outOfOrderProcess.print("BoundedOutOfOrderness");

        // 执行
        env.execute("watermark4streamv1");

        System.out.println("This is watermark4streamv1...done.");
    }
}