package com.test.nkhadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Locale;

public class TestHDFSByFSAPI {
    static {
        // 设置远程操作 hdfs 的用户
        System.setProperty("HADOOP_USER_NAME", "vagrant");
        // 判断当前执行环境是否为 windows,如果是, 设置下载的winutils中的对应版本的 windows hadoop 可执行文件的位置
        // 位置: 可执行文件所在的 bin 目录的所在的位置
        if (System.getProperty("os.name").toLowerCase().contains("windows"))
            System.setProperty("hadoop.home.dir", "c:/dev/hadoop");
    }
    public static void main(String[] args) throws IOException {
        // 给出 hdfs 的根目录的 url String
        String hdfsURL = "hdfs://192.168.43.101:9000/";
//        URL url = new URL(hdfsURL);
        // 借助 URI 创建基于 hdfs 路径的实例
        URI uri = URI.create(hdfsURL);
        // 实例化 HDFS 的配置类, 含有 hadoop 的所有配置的默认项
        Configuration conf = new Configuration();

        // 基于 uri 和 conf, 创建 hdfs file system 的句柄
        // 基于该句柄, 可以实现 hdfs 资源的操作
        FileSystem fs = FileSystem.get(uri, conf);

        // 获取 hdfs 的状态信息
        FsStatus fsStatus = fs.getStatus();
        long used = fsStatus.getUsed();
        long remaining = fsStatus.getRemaining();
        long capacity = fsStatus.getCapacity();

        System.out.println(used + " == " + remaining + " -- " + capacity);

        String coreSiteXml = "hdfs://192.168.43.101:9000/input/core-site.xml";
        // path 实例可以指向 hdfs 或者 本地fs 的某个文件
        Path coreSiteXmlPath = new Path(coreSiteXml);
        Path destPath = new Path("d:/core-site.txt");
        // == hdfs dfs -get 命令
        fs.copyToLocalFile(coreSiteXmlPath, destPath);

        // 借助 hdfs 文件系统的句柄获取指定文件的输入流
        FSDataInputStream is = fs.open(coreSiteXmlPath);
        // 在 控制台输出
        IOUtils.copyBytes(is, System.out, 4096, false);
        IOUtils.closeStream(is);

    }
}
