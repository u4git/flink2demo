package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.util.Collector;

import java.time.Duration;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("This is window4streamv1...");

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

        wordPairs.print("wordPairs");

        /*
        不带 keyby() 的窗口
         */

        // 按时间，滚动窗口
        // wordPairs.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))).sum(1).print("nonkeyed, time, tumbling");

        // 按时间，滑动窗口
        // wordPairs.windowAll(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10),Duration.ofSeconds(1))).sum(1).print("nonkeyed, time, sliding");

        // 按事件，滚动窗口
        // wordPairs.countWindowAll(5).sum(1).print("nonkeyed, event, tumbling");

        // 按事件，滑动窗口
        // wordPairs.countWindowAll(5, 1).sum(1).print("nonkeyed, event, sliding");

        // 会话窗口（只能按时间）
        // wordPairs.windowAll(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(20))).sum(1).print("nonkeyed, session");

        // 全局窗口（事件窗口的底层实现）
        // wordPairs.windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(ContinuousProcessingTimeTrigger.of(Duration.ofSeconds(1)))).sum(1).print("nonkeyed, global");

        /*
        带 keyby() 的窗口
         */

        KeyedStream<Tuple2<String, Integer>, String> wordGroups = wordPairs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        wordGroups.print("wordGroups");

        // 按时间，滚动窗口
        // wordGroups.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))).sum(1).print("keyed, time, tumbling");

        // 按时间，滑动窗口
        // wordGroups.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10),Duration.ofSeconds(1))).sum(1).print("keyed, time, sliding");

        // 按事件，滚动窗口
        wordGroups.countWindow(5).sum(1).print("keyed, event, tumbling");

        // 按事件，滑动窗口
        // wordGroups.countWindow(5, 1).sum(1).print("keyed, event, sliding");

        // 会话窗口（只能按时间）
        // wordGroups.window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(20))).sum(1).print("keyed, session");

        // 全局窗口（事件窗口的底层实现）
        // wordGroups.window(GlobalWindows.create()).trigger(PurgingTrigger.of(ContinuousProcessingTimeTrigger.of(Duration.ofSeconds(1)))).sum(1).print("keyed, global");

        // 执行

        env.execute("window4streamv1");

        System.out.println("This is window4streamv1...done.");
    }
}