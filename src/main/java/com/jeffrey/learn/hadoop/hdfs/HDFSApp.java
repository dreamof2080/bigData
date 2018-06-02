package com.jeffrey.learn.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Jeffrey.Liu
 * @version V1.0
 * @Description
 * @date 2018-6-3 0:38
 */
public class HDFSApp {
    private static final String HDFS_PATH = "hdfs://192.168.56.121:9000";
    private FileSystem fileSystem;
    private Configuration configuration;

    public HDFSApp() throws URISyntaxException, IOException {
        this.configuration = new Configuration();
        this.fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration);
    }

    /**
     * 创建HDFS目录
     */
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    public void create() throws IOException {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 查看文件
     * @throws IOException
     */
    public void cat() throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(inputStream,System.out,1024);
        inputStream.close();
    }

    public void tearDown(){
        this.configuration=null;
        this.fileSystem=null;
    }
}
