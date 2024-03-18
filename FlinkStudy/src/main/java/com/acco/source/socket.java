package com.acco.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: socket
 * Description: None
 * Package: com.acco.source
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-24 22:45
 */
public class socket {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 7777);
    }
}
