package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 运行时需要添加以下 JVM 参数解决模块访问问题：
 * --add-opens java.base/java.util=ALL-UNNAMED
 * --add-opens java.base/java.lang=ALL-UNNAMED
 * --add-opens java.base/java.lang.reflect=ALL-UNNAMED
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

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

        // 统计
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Tuple2<String, Integer>> wordCount = wordPairs.process(new OneInputStreamProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private Map<String, Integer> wordCounts;
            private Collector<Tuple2<String, Integer>> collector;

            @Override
            public void open(NonPartitionedContext<Tuple2<String, Integer>> ctx) throws Exception {
                OneInputStreamProcessFunction.super.open(ctx);
                this.wordCounts = new HashMap<>();
            }

            @Override
            public void processRecord(Tuple2<String, Integer> record, Collector<Tuple2<String, Integer>> output, PartitionedContext<Tuple2<String, Integer>> ctx) throws Exception {
                String word = record.f0;
                int count = wordCounts.getOrDefault(word, 0) + record.f1;
                this.wordCounts.put(word, count);
                this.collector = output;
            }

            @Override
            public void endInput(NonPartitionedContext<Tuple2<String, Integer>> ctx) throws Exception {
                // 输出所有单词的最终统计结果
                if (this.collector != null) {
                    for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                        this.collector.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                    }
                }
            }
        });

        // 打印结果
        wordCount.toSink(DataStreamV2SinkUtils.wrapSink(new Sink<Tuple2<String, Integer>>() {
            @Override
            public SinkWriter<Tuple2<String, Integer>> createWriter(WriterInitContext context) throws IOException {
                return new SinkWriter<Tuple2<String, Integer>>() {
                    @Override
                    public void write(Tuple2<String, Integer> element, Context context) {
                        System.out.println(element);
                    }

                    @Override
                    public void flush(boolean endOfInput) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        }));

        // 执行
        env.execute("wordcount4batchv2");

        System.out.println("This is flink2demo wordcount4batchv2...done.");
    }
}