package com.eslink.hdfsDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName HdfsApiTests
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/15 17:33
 * @Version 1.0
 **/
public class HdfsApiTests {

    /*4.6.1上传文件*/
    @Test
    public void putFile() throws IOException, InterruptedException {
        String local = "E:\\IdeaProjects\\study\\hadoop-study\\hdfsDemo\\src\\main\\resources\\word2.txt";
        String dest = "hdfs://server-1:9000/user/root/input/word2.txt";
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dest), cfg, "root");
        fs.copyFromLocalFile(new Path(local), new Path(dest));
        fs.close();
    }

    /*4.6.2下载文件*/
    @Test
    public void getFile() throws IOException, InterruptedException {
        String hdfsPath = "hdfs://server-1:9000/user/root/input/words.txt";
        String localPath = "E:\\IdeaProjects\\study\\hadoop-study\\hdfsDemo\\src\\main\\resources\\copy_words.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf, "root");
        fs.copyToLocalFile(false, new Path(hdfsPath), new Path(localPath), true);
        fs.close();

    }
}
