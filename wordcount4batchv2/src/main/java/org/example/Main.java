package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("This is flink2demo wordcount4batchv2...");

        // 创建环境

        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        env.setExecutionMode(RuntimeExecutionMode.BATCH);

        // 读取数据

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("words.txt")).build();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<String> lines = env.fromSource(DataStreamV2SourceUtils.wrapSource(fileSource), "LocalTextFile");

        // 分词

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Tuple2<String, Integer>> wordPairs = lines.process(new OneInputStreamProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processRecord(String record, Collector<Tuple2<String, Integer>> output, PartitionedContext<Tuple2<String, Integer>> ctx) throws Exception {
                String[] words = record.split(" ");

                for (String word : words) {
                    Tuple2<String, Integer> t = Tuple2.of(word, 1);
                    output.collect(t);
                }
            }
        });

        // 分组

        KeyedPartitionStream<String, Tuple2<String, Integer>> wordGroups = wordPairs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 统计

        KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream<String, Tuple2<String, Integer>> wordCount = wordGroups.process(
                new OneInputStreamProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private int count;

                    @Override
                    public void open(NonPartitionedContext<Tuple2<String, Integer>> ctx) throws Exception {
                        OneInputStreamProcessFunction.super.open(ctx);
                        this.count = 0;
                    }

                    @Override
                    public void processRecord(Tuple2<String, Integer> record, Collector<Tuple2<String, Integer>> output, PartitionedContext<Tuple2<String, Integer>> ctx) throws Exception {
                        this.count += record.f1;
                        output.collect(Tuple2.of(record.f0, this.count));
                    }
                },
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        // 打印结果
        wordCount.toSink((Sink<Tuple2<String, Integer>>) new PrintSink<Tuple2<String, Integer>>());

        // 执行
        env.execute("wordcount4batchv2");

        System.out.println("This is flink2demo wordcount4batchv2...done.");
    }
}