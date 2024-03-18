package com.acco.WaterMark;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * ClassName: MyPartitioner
 * Description: None
 * Package: com.acco.WaterMark
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-28 13:10
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
