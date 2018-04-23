package com.music.musicstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * <p>
 * description:
 * </p>
 * Created on 2017/6/7 17:32
 *
 * @author leiguang
 */
@Repository
public class HdfsDao {

    private static Logger LOGGER = LoggerFactory.getLogger(HdfsDao.class);

    @Value("${hdfs.path}")
    private String hdfs_path;

    @Value("${hdfs.file_home}")
    private String file_home;

    @Value("${hdfs.user}")
    private String user;

    public void setHdfs_path(String hdfs_path) {
        this.hdfs_path = hdfs_path;
    }

    public void setFile_home(String file_home) {
        this.file_home = file_home;
    }

    private FileSystem fs;


    @PostConstruct
    public void init() throws IOException, URISyntaxException, InterruptedException {
        this.fs = FileSystem.get( new URI(hdfs_path),  new Configuration(), user);
    }


    public void createFile(String fileName) {
        try {
            Path path = new Path(file_home + fileName);
            //创建不存在的文件
            if(!fs.exists(path)) {
                FSDataOutputStream os_temp = fs.create(path);
                os_temp.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public OutputStreamWriter createOutputStreamWriter(String fileName) {
        FSDataOutputStream os = null;
        try {
            Path path = new Path(file_home + fileName);
            //多线程对单文件写操作，需要append追加内容
            os = fs.append(path);
            return new OutputStreamWriter(os, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public synchronized void writeToHDFS(List<String> list, OutputStreamWriter osw) {
        for (String str : list) {
            write(str + "\n", osw);
        }
    }

    public void write(String data, OutputStreamWriter osw) {
        try {
            osw.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void closeWriter(OutputStreamWriter osw) {
        try {
            if(null != osw) {
                osw.flush();
                osw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        /*if (null != osw)
            try {
                osw.flush();
                osw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }*/

        if (null != fs)
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    /*public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        HdfsDao hdfsDao = new HdfsDao();
        hdfsDao.setHdfs_path("hdfs://192.168.1.60:9000");
        hdfsDao.setFile_home("/usr/lg/music/");
        hdfsDao.init();
        hdfsDao.createOutputStreamWriter("test1.txt");
        List<String> test = new ArrayList<>();
        test.add("2222222222222");
        test.add("3333333333333");
        test.add("4444444444444");
        test.add("5555555555555");
        hdfsDao.writeToHDFS(test);
        hdfsDao.close();
    }*/

}
