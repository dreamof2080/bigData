package com.jeffrey.learn.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
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

    /**
     * 重命名
     */
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    /**
     * 上传文件到HDFS
     */
    public void copyFromLocalFile() throws IOException {
        Path localPath = new Path("D:\\download\\5950cae200010ca100000000.rar");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传文件到HDFS
     */
    public void copyFromLocalFileWithProgross() throws IOException {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("D:\\download\\hadoop-3.1.0.tar.gz")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/hadoop-3.1.0.tar.gz"), new Progressable() {
            @Override
            public void progress() {
                //带进度提醒信息
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(inputStream,outputStream,4096);
        inputStream.close();
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 下载文件
     */
    public void copyToLocalFile() throws IOException {
        Path localPath = new Path("D:\\tmp\\b.txt");
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    /**
     * 查看某个目录下的所有文件
     */
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory()?"文件夹":"文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir+"\t"+replication+"\t"+len+"\t"+path);
        }
    }

    /**
     * 删除
     */
    public void delete() throws IOException {
        fileSystem.delete(new Path("/hdfsapi/test/b.txt"),true);
    }

    public void tearDown(){
        this.configuration=null;
        this.fileSystem=null;
    }
}
