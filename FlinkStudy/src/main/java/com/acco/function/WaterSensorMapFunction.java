package com.acco.function;

import com.acco.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * ClassName: WaterSensorMapFunction
 * Description: None
 * Package: com.acco.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-27 21:37
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        WaterSensor waterSensor = new WaterSensor();
        String[] split = value.split(",");
        waterSensor.setId(split[0]);
        waterSensor.setTs(Long.valueOf(split[1]));
        waterSensor.setVc(Integer.valueOf(split[2]));
        return waterSensor;
    }
}

