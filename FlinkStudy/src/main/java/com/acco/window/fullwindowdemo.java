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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ClassName: fullwindowdemo
 * Description: None
 * Package: com.acco.window
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-27 22:19
 */
public class fullwindowdemo {
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
        SingleOutputStreamOperator<Integer> process = window.process(
                /**
                 *
                 Type parameters:
                 <IN> – The type of the input value. 输入
                 <OUT> – The type of the output value. 输出
                 <KEY> – The type of the key. 分组的key
                 <W> – The type of Window that this window function can be applied on. 上下文吧
                 */
                new ProcessWindowFunction<WaterSensor, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Integer> out) throws Exception {
                        Integer a = 0;
                        for (WaterSensor element : elements) {
                            System.out.println(element);
                            a = a + element.getVc();
                        }
                        out.collect(a);
                    }
                }
        );
        process.printToErr();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
