package com.acco.transform;

import com.acco.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: sumTEst
 * Description: None
 * Package: com.acco.transform
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-25 23:15
 */
public class sumTEst {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1l, 2),
                new WaterSensor("s1", 2l, 3),
                new WaterSensor("s1", 3l, 4),
                new WaterSensor("s1", 4l, 5)

        );
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {

            @Override
            public String getKey(WaterSensor value) throws Exception {

                return value.getId();
            }
        });
        SingleOutputStreamOperator<WaterSensor> vc = waterSensorStringKeyedStream.sum("vc");
        vc.print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
