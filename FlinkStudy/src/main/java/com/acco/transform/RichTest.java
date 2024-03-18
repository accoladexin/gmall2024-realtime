package com.acco.transform;

import com.acco.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: RichTest
 * Description: None
 * Package: com.acco.transform
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-26 21:32
 */
public class RichTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1l, 2),
                new WaterSensor("s1", 2l, 3),
                new WaterSensor("s1", 3l, 4),
                new WaterSensor("s1", 4l, 5)

        );

        waterSensorDataStreamSource.map(
                new RichMapFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("open");
                        RuntimeContext runtimeContext = getRuntimeContext();


                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("close");
                    }

                    @Override
                    public WaterSensor map(WaterSensor value) throws Exception {
                        System.out.println(value);
                        return value;
                    }
                }
        ).setParallelism(1);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
