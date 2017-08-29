package com.org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class Hadoop {

    public  void uploadInputFile(String localFile) throws IOException{
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://192.168.1.61:8020/";
        String hdfsInput = "hdfs://192.168.1.61:8020/user/hadoop";
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(localFile), new Path(hdfsInput));
        fs.close();
        System.out.println("已经上传文件到input文件夹啦");
    }

    public static void main(String[] args)  throws IOException{
        Hadoop  hadoop = new Hadoop();
        hadoop.uploadInputFile("/Users/ghy/Desktop/input.txt");
    }

}
