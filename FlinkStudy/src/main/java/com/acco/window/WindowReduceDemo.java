package com.acco.window;

import com.acco.bean.WaterSensor;
import com.acco.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * ClassName: WindowReduceDemo
 * Description: None
 * Package: com.acco.window
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-27 21:36
 */
public class WindowReduceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(r -> r.getId())
                // 设置滚动事件时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WaterSensor>() {

                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("调用reduce方法，之前的结果:"+value1 + ",现在来的数据:"+value2);
                        return new WaterSensor(value1.getId(), System.currentTimeMillis(),value1.getVc()+value2.getVc());
                    }
                })
                .print();

        env.execute();
    }
}
