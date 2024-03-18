package com.acco.transform;

import com.acco.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;

/**
 * ClassName: maptest
 * Description: None
 * Package: com.acco.transform
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-24 23:05
 */
public class maptest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> Waterstream = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s3", 1L, 1)
                );
        // TODO mapfunction
        // 匿名实现类
//        SingleOutputStreamOperator<String> mapsource = Waterstream.map(
//                new MapFunction<WaterSensor, String>() {
//                    @Override
//                    public String map(WaterSensor value) throws Exception {
//                        return value.getId();
//                    }
//                }
//        );

        // lambda表达式
//        SingleOutputStreamOperator<String> mapsource = Waterstream.map(sensor-> sensor.getId());
        // 正式的写法
        SingleOutputStreamOperator<String> map = Waterstream.map(new myMapfunction());


        map.print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public static class  myMapfunction implements MapFunction<WaterSensor, String>{
        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
