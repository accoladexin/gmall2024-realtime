package com.acco.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: kafkasource
 * Description: None
 * Package: com.acco.source
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-24 22:47
 */
public class kafkasource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkasource = KafkaSource.<String>builder() //泛型 返回的类型
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setGroupId("acco") //消费者组的id
                .setTopics("topic_db")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest()) //earliest(默认)最早消费 latest 最新消费 offsets 提交偏移量
                .build();
        DataStreamSource<String> kafkaString = env.fromSource(kafkasource, WatermarkStrategy.noWatermarks(), "kafkasource");

    }
}
