package com.acco.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: wordcountstreamDemo
 * Description: None
 * Package: com.acco.wc
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-21 22:25
 */
public class wordcountstreamDemo{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> lineDS= env.readTextFile("E:\\JavaStudy\\gmall2024-realtime\\gmall2024-realtime\\FlinkStudy\\word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordandone = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(

        ) {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordandone = Tuple2.of(word, 1);
                    out.collect(wordandone);
                }
            }
        });
        // TODO 聚合
        KeyedStream<Tuple2<String, Integer>, String> wordgroup = wordandone.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                // 通过第一位置的东西来聚合，然后往下面继续是String（即分组的东西）
                return value.f0;
            }
        });
        wordgroup.sum(1).print();
        env.execute();
    }
}
