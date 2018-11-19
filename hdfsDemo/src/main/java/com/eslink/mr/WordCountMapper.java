package com.eslink.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @ClassName WordCountMapper
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/16 13:53
 * @Version 1.0
 **/
//4个泛型参数：前两个表示map的输入键值对的key和value的类型，后两个表示输出键值对的key和value的类型
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //该方法循环调用，从文件的split中读取每行调用一次，把该行所在的下标为key，该行的内容为value
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        System.out.println("key=" + key + ",value=" + value);
        String[] words = StringUtils.split(value.toString(), ' ');
        for (String w : words) {
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
