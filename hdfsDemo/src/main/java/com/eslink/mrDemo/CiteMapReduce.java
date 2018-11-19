package com.eslink.mrDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName CiteMapReduce
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/19 10:54
 * @Version 1.0
 **/
public class CiteMapReduce extends Configured implements Tool {

    public static class MapClass extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String csv = "";
            for (Text val : values) {
                if (csv.length() > 0) csv += ",";
                csv += val.toString();
            }
            context.write(key, new Text(csv));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.defaultFS", "hdfs://server-1:9000");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(CiteMapReduce.class); //主类

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileSystem hdfs = out.getFileSystem(conf);
        if (hdfs.isDirectory(out)) {
            hdfs.delete(out, true);
        }

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.setProperty("HADOOP_USER_NAME", "root");
        try {
            String[] args0 = {"/user/root/cite/input/cite75_99.txt", "/user/root/cite/output/"};
            int res = ToolRunner.run(new Configuration(), new CiteMapReduce(), args0);
//            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("耗时：" + (System.currentTimeMillis() - start) + " ms");
        }
    }
}
