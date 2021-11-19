package com.hunter95.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        //配置副本数为2
        configuration.set("dfs.replication", "2");
        //用户
        String user = "hunter95";
        //获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //关闭资源
        fs.close();
    }

    /**
     * 参数优先级
     * hdfs-default.xml < hdfs-site.xml < 在项目资源目录下的配置文件 < 代码里面的配置
     */

    //创建目录
    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {
        //创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    //上传
    @Test
    public void testPut() throws IOException {
        //参数解读：
        // 参数一：表示删除原数据；参数二：表示是否允许覆盖；
        // 参数三：原数据路径；参数四：目的地路径
        fs.copyFromLocalFile(false, true, new Path("D:\\sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    //文件下载
    @Test
    public void testGet() throws IOException {
        //参数解读：
        // 参数一：原文件是否删除；参数二：HDFS原文件路径；
        // 参数三：目标地址路径Win；参数四：
        fs.copyToLocalFile(true, new Path("hdfs://hadoop102/xiyou/huaguoshan"), new Path("D:\\"), true);
    }

    //删除
    @Test
    public void testRm() throws IOException {
        //参数解读：
        // 参数一：要删除的路径；参数二：是否递归删除；

        //一、删除文件
        //fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"),false);

        //二、删除空目录
        //fs.delete(new Path("/xiyou"),false);

        //三、删除非空目录
        fs.delete(new Path("/jinguo"), true);
    }

    //文件的更名和移动
    @Test
    public void testMv() throws IOException {
        //参数解读：
        // 参数一：原文件路径；参数二：目标文件路径;

        //一、对文件名称修改
        //fs.rename(new Path("/input/word.txt"),new Path("/input/ss.txt"));

        //二、对文件的移动和更名
        //fs.rename(new Path("/input/ss.txt"),new Path("/hunter.txt"));

        //三、目录的更名
        //fs.rename(new Path("/input"), new Path("/output"));
    }

    //获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        //参数解读：
        // 参数一：要查看的路径；参数二：是否递归;

        //一、获取所有文件信息(返回迭代器)
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("======" + fileStatus.getPath() + "======");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //二、获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        //参数解读：
        // 参数一：要查看的路径；

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        //循环遍历每一个对象，判断是否是文件，并打印信息
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("目录：" + status.getPath().getName());
            }
        }
    }
}
