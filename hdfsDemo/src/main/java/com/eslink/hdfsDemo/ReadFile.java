package com.eslink.hdfsDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @ClassName ReadFile
 * @Description TODO
 * @Author zeng.yakun (0178)
 * @Date 2018/11/15 16:58
 * @Version 1.0
 **/
public class ReadFile {

    public static void main(String[] args) throws IOException {
        String uri = "hdfs://server-1:9000/user/root/input/words.txt";
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), cfg);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
