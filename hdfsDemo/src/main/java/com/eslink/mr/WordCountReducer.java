package com.eslink.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName WordCountReducer
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/16 13:57
 * @Version 1.0
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Map过程输出<key,values>中key为单个单词，而values是对应单词的计数值所组成的列表，Map的输出就是Reduce的输入，
     * 每组调用一次，这一组数据特点：key相同，value可能有多个。
     * /所以reduce方法只要遍历values并求和，即可得到某个单词的总次数。
     */
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        System.out.println("key=" + key + ",values=" + values);
        int sum = 0;
        for (IntWritable i : values) {
            sum = sum + i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
