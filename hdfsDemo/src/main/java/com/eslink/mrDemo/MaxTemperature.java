package com.eslink.mrDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName MaxTemperature
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/19 11:46
 * @Version 1.0
 **/
public class MaxTemperature extends Configured implements Tool {

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int MISSING = 9999;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println("key=" + key + ",value=" + value);
            String year = line.substring(15, 19);
            int airTemperature;
            if (line.charAt(87) == '+') {
                airTemperature = (int) Double.parseDouble(line.substring(88, 92).trim());
            } else {
                airTemperature = (int) Double.parseDouble(line.substring(87, 92).trim());
            }
            String quality = line.substring(92, 93);
            if (airTemperature != MISSING && quality.matches("[01459]")) {
                context.write(new Text(year), new IntWritable(airTemperature));
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new IntWritable(maxValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.defaultFS", "hdfs://server-1:9000");
        Job job = Job.getInstance(conf, "MaxTemperature");
        job.setJarByClass(MaxTemperature.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileSystem hdfs = out.getFileSystem(conf);
        if (hdfs.isDirectory(out)) {
            hdfs.delete(out, true);
        }

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.setProperty("HADOOP_USER_NAME", "root");
        try {
            String[] args0 = {"/user/root/temperature/input", "/user/root/temperature/output"};
            int res = ToolRunner.run(new Configuration(), new MaxTemperature(), args0);
            System.out.println("res=" + res + ",cost=" + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
