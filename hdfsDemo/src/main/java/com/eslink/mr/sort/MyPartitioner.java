package com.eslink.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * <Text,NullWritable>是Mapper的输出类型
 *
 * @author hadron
 */
public class MyPartitioner extends HashPartitioner<Text, NullWritable> {
    //执行时间越短越好
    public int getPartition(Text key, NullWritable value, int numReduceTasks) {
        return (key.toString().split("\t")[2].hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
