package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("This is flink2demo wordcount4streamv1...");

        // 创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 读取数据

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("words.txt")).build();

        DataStreamSource<String> lines = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "LocalTextFile");

        // 分词

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordPairs = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");

                for (String word : words) {
                    Tuple2<String, Integer> t = Tuple2.of(word, 1);

                    out.collect(t);
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
        env.execute();

        System.out.println("This is flink2demo wordcount4streamv1...done.");
    }
}