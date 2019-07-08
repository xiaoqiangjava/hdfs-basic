package com.xq.learn.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS 的api操作
 * @author xiaoqiang
 * @date 2019/7/8 23:23
 */
public class HdfsDemo
{
    /**
     * 获取hdfs的客户端
     */
    @Test
    public void getClient() throws IOException
    {
        // 运行时需要指定用户，通过命令行参数：-DHADOOP_USER_NAME=root，每次运行时都需要指定
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://learn:9000");
        // 获取HDFS的客户端
        FileSystem fs = FileSystem.get(conf);
        // 创建目录
        fs.mkdirs(new Path("/learn/mkdir/aaa"));
        // 关闭资源
        fs.close();
    }

    /**
     * 获取HDFS客户端优化：在获取文件系统时可以指定user
     */
    @Test
    public void getClient2() throws URISyntaxException, IOException, InterruptedException
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://learn:9000");
        // 获取HDFS客户端
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 创建一个目录, 会递归创建目录
        fs.mkdirs(new Path("/learn/mkdir/bbb"));
        // 关闭资源
        fs.close();
    }

    /**
     * 测试上传文件：使用的配置文件是默认的配置文件，在集群中设置的配置文件只有在使用集群启动时才会生效
     * 可以在classpath下面新建一个hdfs-site.xml文件，使用指定的配置
     * 配置文件的优先级：
     * hadoop-hdfs-2.7.5.jar/hdfs-default.xml < classpath: hdfs-site.xml < conf.set("", "")指定的配置
     * 文件上传不是put方法，是copy系列
     * 如果指定的目标文件是一个存在的文件名称，则会重命名文件为目标指定的文件名
     * @throws Exception
     */
    @Test
    public void testPut() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        // 获取HDFS客户端
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 上传文件
        fs.copyFromLocalFile(new Path("D:\\data\\a.txt"), new Path("/learn/mkdir"));
        // 关闭资源
        fs.close();
    }

    /**
     * 文件下载：文件下载不是get，是copy系列，文件不存在时会报错：FileNotFoundException
     * @throws Exception
     */
    @Test
    public void testGet() throws Exception
    {
        Configuration conf = new Configuration();
        // 获取HDFS客户端
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 下载文件, useRawLocalFileSystem参数的作用是是否开启文本校验，取值为false是会生成CRC文件
        fs.copyToLocalFile(false, new Path("/learn/mkdir/a.txt"), new Path("D:\\"), true);
        // 关闭资源
        fs.close();
    }

    /**
     * 测试文件的删除, delete如果不设置递归删除时，不能删除不为空的目录
     */
    @Test
    public void testDel() throws Exception
    {
        // 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 删除文件
        fs.delete(new Path("/learn/mkdir"), true);
        // 释放资源
        fs.close();
    }

    /**
     * 重命名文件
     * @throws Exception
     */
    @Test
    public void testRename() throws Exception
    {
        // 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 重命名文件
        fs.rename(new Path("/learn/mkdir/a.txt"), new Path("/learn/mkdir/b.txt"));
        // 释放资源
        fs.close();
    }

    /**
     * 查询文件信息: 文件名称，大小，block信息等
     * @throws Exception
     */
    @Test
    public void testLs() throws Exception
    {
        // 获取文件客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 查询文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext())
        {
            LocatedFileStatus fileStatus = listFiles.next();
            // 文件名称
            System.out.println("名称： " + fileStatus.getPath().getName());
            // 文件大小
            System.out.println("大小：" + fileStatus.getLen());
            // 获取文件权限
            System.out.println("权限：" + fileStatus.getPermission());
            // 获取文件block信息，一个文件可以被分成多个块，所以返回一个列表
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation block : blockLocations)
            {
                // 一个文件存在多个副本，所以host是一个数组
                String[] hosts = block.getHosts();
                for (String host : hosts)
                {
                    System.out.println("block的host: " + host);
                }
                System.out.println("================================");
            }

        }
        // 释放资源
        fs.close();
    }

    /**
     * 判断文件是文件还是文件夹
     */
    @Test
    public void testLs2() throws Exception
    {
        // 获取HDFS客户端
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://learn:9000"), conf, "root");
        // 获取文件状态，判断是文件还是文件夹, 不会递归列出文件信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus status : fileStatuses)
        {
            String fileName = status.getPath().getName();
            if (status.isDirectory())
            {
                System.out.println(fileName + " is a directory.");
            }
            if (status.isFile())
            {
                System.out.println(fileName + " is a file.");
            }
        }
        // 关闭资源
        fs.close();
    }
}
