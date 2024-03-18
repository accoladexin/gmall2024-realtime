package com.acco.WaterMark;

import com.acco.bean.WaterSensor;
import com.acco.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: testWatermark
 * Description: None
 * Package: com.acco.WaterMark
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-28 12:53
 */
public class testWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 7777);

        DataStream<WaterSensor> waterSensorDataStream = hadoop102
                .map(new WaterSensorMapFunction() {
                })
                .partitionCustom(
                        new MyPartitioner(), new KeySelector<WaterSensor, String>() {
                            @Override
                            public String getKey(WaterSensor value) throws Exception {
                                return String.valueOf(value.getVc());
                            }
                        }
                );
        waterSensorDataStream.print();
        // 定义生成Watermark

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


