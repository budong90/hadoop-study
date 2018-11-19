package com.eslink.mrJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
    public static void main(String[] args) {
        // 设置环境变量HADOOP_USER_NAME，其值是root
        System.setProperty("HADOOP_USER_NAME", "root");
        // Configuration类包含了Hadoop的配置
        Configuration config = new Configuration();
        // 设置fs.defaultFS
        config.set("fs.defaultFS", "hdfs://server-1:9000");
        // 设置yarn.resourcemanager节点
        config.set("yarn.resourcemanager.hostname", "server-1");
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);
            job.setJobName("JoinDemo");
            // 设置Mapper和Reducer类
            job.setMapperClass(JoinMapper.class);
            job.setReducerClass(JoinReducer.class);
            // 设置reduce方法输出key和value的类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 指定输入输出路径
            FileInputFormat.addInputPath(job, new Path("/user/root/input/dept.txt"));
            FileInputFormat.addInputPath(job, new Path("/user/root/input/emp.txt"));
            Path outpath = new Path("/user/root/output/");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
            // 提交任务，等待执行完成
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
