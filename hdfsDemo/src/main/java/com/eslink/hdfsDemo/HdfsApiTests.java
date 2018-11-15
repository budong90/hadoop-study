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
        // 不加第一个和第四个参数会报错:
        // java.io.IOException: (null) entry in command string: null chmod 0644
        fs.copyToLocalFile(false, new Path(hdfsPath), new Path(localPath), true);
        fs.close();
    }

    /*4.6.3创建HDFS目录*/
    @Test
    public void createDir() throws IOException, InterruptedException {
        String url = "hdfs://server-1:9000/tmp/";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf, "root");
        boolean b = fs.mkdirs(new Path(url));
        System.out.println(b);
        fs.close();
    }

    /*4.6.4删除文件或目录*/
    @Test
    public void deleteFile() throws IOException, InterruptedException {
        String uri = "hdfs://server-1:9000/tmp";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf, "root");
        boolean b = fs.delete(new Path(uri));
        System.out.println(b);
        fs.close();
    }

    /*4.6.5下载HDFS目录*/
    @Test
    public void copyToLocalFile() throws IOException, InterruptedException {
        String hdfsPath = "hdfs://server-1:9000/user/root/input";
        String localPath = "E:\\IdeaProjects\\study\\hadoop-study\\hdfsDemo\\src\\main\\resources\\";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf, "root");
        fs.copyToLocalFile(false, new Path(hdfsPath), new Path(localPath), true);
//        fs.copyToLocalFile(false, new Path(hdfsPath), new Path(localPath));
        fs.close();
    }

    /*4.6.6上传本地目录*/
    @Test
    public void copyFromLocalFile() throws IOException, InterruptedException {
        String hdfsPath = "hdfs://server-1:9000/user/root/";
        String localPath = "E:\\IdeaProjects\\study\\hadoop-study\\hdfsDemo\\src\\main\\resources\\words";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf, "root");
        fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        fs.close();
    }
}
