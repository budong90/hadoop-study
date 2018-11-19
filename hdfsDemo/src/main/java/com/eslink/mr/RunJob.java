package com.eslink.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @ClassName RunJob
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/16 14:14
 * @Version 1.0
 **/
public class RunJob {

    public static void main(String[] args) {
        //设置环境变量HADOOP_USER_NAME
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration config = new Configuration();
        //设置fs.defaultFS
        config.set("fs.defaultFS", "hdfs://server-1:9000");
        //设置yarn.resourcemanager
        config.set("yarn.resourcemanager.hostname", "server-1");
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);
            job.setJobName("wc");
            //设置Mapper类
            job.setMapperClass(WordCountMapper.class);
            //设置Reduce类
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path("/user/root/input/"));
            Path outpath = new Path("/user/root/output/");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
