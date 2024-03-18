package com.acco.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: WordCountBatchDemo
 * Description: None
 * Package: com.acco.wc
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-21 21:41
 */
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // TODO 2.读数据
        DataSource<String> lineDS = env.readTextFile("E:\\JavaStudy\\gmall2024-realtime\\gmall2024-realtime\\FlinkStudy\\word.txt");
        // TODO 3.数据处理

        FlatMapOperator<String, Tuple2<String, Integer>> wordandone = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);
                    collector.collect(wordTuple);
                }

            }
        });
        // TODO 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordgoroup = wordandone.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> wordsum = wordgoroup.sum(1);
        wordsum.print();
        // TODE execute

    }
}
