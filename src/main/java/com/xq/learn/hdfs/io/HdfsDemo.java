package com.xq.learn.hdfs.io;

import com.sun.xml.internal.rngom.util.Uri;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URI;

/**
 * HDFS的IO流操作
 * @author xiaoqiang
 * @date 2019/7/9 1:11
 */
public class HdfsDemo
{
    /**
     * 文件上传: 通过fs创建文件输出流，使用Hadoop提供的IOUtils工具类实现流的拷贝
     * fs.create()方法创建输出流
     */
    @Test
    public void testPut() throws Exception
    {
        // 1. 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 2. 创建输入流
        FileInputStream inputStream = new FileInputStream(new File("D:\\data\\a.txt"));
        // 3. 创建输出流
        FSDataOutputStream outputStream = fs.create(new Path("/learn/mkdir/aa.txt"));
        // 4. 流的拷贝
        IOUtils.copyBytes(inputStream, outputStream, conf);
        // 5. 关闭资源
        fs.close();
    }

    /**
     * 文件的下载: 输入流有fs文件系统提供
     * fs.open()方法获取输入流
     * @throws Exception
     */
    @Test
    public void testGet() throws Exception
    {
        // 1. 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 2. 创建输入流
        FSDataInputStream inputStream = fs.open(new Path("/learn/mkdir/aa.txt"));
        // 3. 创建输出流
        OutputStream outputStream = new FileOutputStream("D:\\aa.txt");
        // 4. 流的拷贝
        IOUtils.copyBytes(inputStream, outputStream, conf);
        // 5. 关闭资源
        fs.close();
    }

    /**
     * 文件下载： 下载第一块文件: 只下载128M
     */
    @Test
    public void testGetBlock() throws Exception
    {
        // 1. 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 2. 获取输入流
        FSDataInputStream inputStream = fs.open(new Path("/learn/hadoop.tar.gz"));
        // 3. 获取输出流
        FileOutputStream outputStream = new FileOutputStream("D:\\hadoop.part1");
        // 4. 流的拷贝
        BufferedOutputStream bos = new BufferedOutputStream(outputStream);
        byte[] buff = new byte[1024];
        for (int i = 0; i < 128 * 1024; i++)
        {
            int len = inputStream.read(buff);
            bos.write(buff, 0, len);
        }
        bos.flush();
        // 5. 释放资源
        fs.close();
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(bos);
    }

    /**
     * 获取第二块: FSDataInputStream提供了指针操作，可以通过移动指针来获取流
     * fsDataInputStream.seek()移动指针
     * @throws Exception
     */
    @Test
    public void testGetBlock1() throws Exception
    {
        // 1. 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 2. 获取输入流
        FSDataInputStream inputStream = fs.open(new Path("/learn/hadoop.tar.gz"));
        // 3. 获取输出流
        FileOutputStream outputStream = new FileOutputStream("D:\\hadoop.part2");
        // 4. 流的拷贝
        // 移动指针到第二个块
        inputStream.seek(1024*1024*128);
        IOUtils.copyBytes(inputStream, outputStream, conf);
        // 5. 释放资源
        fs.close();
    }

}
