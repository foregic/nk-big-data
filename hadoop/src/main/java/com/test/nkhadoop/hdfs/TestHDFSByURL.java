package com.test.nkhadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

// 操作: HDFS: /input/core-site.xml
// hdfs://192.168.43.101:9000/input/core-site.xml
public class TestHDFSByURL {
    // 开启 HDFS 协议支持, 保证 URL 可以连接并打开 hdfs 上一个流
    // 只需在 jvm 中注册一次, 所以放在 static code 中.
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    // 指定 hdfs 协议格式的 url, 可以在 hosts 文件配置 master 和 ipaddress 映射, rpc 端口是 9000
    private static final String hdfsURL = "hdfs://192.168.43.101:9000/input/core-site.xml";

    public static void main(String[] args) throws IOException {
        URL url = new URL(hdfsURL);
        // 获取输入流, 基于 hdfs 协议的 IOStream
        // InputStream is = url.openStream();  // 直接获取
        // 获取 url 连接, 并基于连接获取 IOStream, 基于 IOSteam 就可以完成读写(对 HDFS 的上传&下载)
        URLConnection urlConnection = url.openConnection();
        InputStream is = urlConnection.getInputStream();

        // 借助 hadoop 的工具类实现 IOStream 的操作
        IOUtils.copyBytes(is, System.out, 4096, false);
        // 借助 hadoop 的工具类关闭流
        IOUtils.closeStream(is); //is.close();
    }
}
