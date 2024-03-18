package com.acco.partition;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: shuffletest
 * Description: None
 * Package: com.acco.partition
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-26 21:40
 */
public class shuffletest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Integer> filter = integerDataStreamSource.filter(
                new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2 == 0;
                    }
                }
        );
        filter.print();
        integerDataStreamSource.filter(value -> value % 2 == 1).print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
