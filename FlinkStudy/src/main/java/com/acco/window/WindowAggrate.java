package com.acco.window;

import com.acco.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * ClassName: WindowAggrate
 * Description: None
 * Package: com.acco.window
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-27 22:01
 */
public class WindowAggrate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 7777);
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = hadoop102.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] s = value.split(",");
                return new WaterSensor(s[0], Long.valueOf(s[1]), Integer.valueOf(s[2]));
            }
        }).keyBy(new KeySelector<WaterSensor, String>() {

            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        WindowedStream<WaterSensor, String, TimeWindow> window = waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<String> aggregate = window.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                // 初始化
                System.out.println("11111111");
                return 10;
            }

            @Override
            public Integer add(WaterSensor value, Integer accumulator) { // 输入和累加器
                // 聚合逻辑
                System.out.println(value);
                return value.getVc() + accumulator;
            }

            @Override
            public String getResult(Integer accumulator) {

                return String.valueOf(accumulator);
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        });
        aggregate.printToErr();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
