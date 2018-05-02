package com.music.musicstore;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * <p>
 * description:
 * </p>
 * Created on 2018/4/13 16:30
 *
 * @author leiguang
 */
//@Component
public class MusicConsumer {

    @Autowired
    private HdfsDao hdfsDao;

    //写入到hdfs中的一个文件的长度
    static long MAX_SIZE = 0;

    private ExecutorService es = new ThreadPoolExecutor(5, 5,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    List<String> container = new ArrayList<>();

    long length = 0;

    private int id;

    //@KafkaListener(id = "test", topics = "my-mutil-topics")
    public void consoumer(String value){
        if (!StringUtils.isBlank(value)){
            container.add(value);
            length += value.length();
            if (length >= MAX_SIZE){
                List<String> temp = container;
                container = new ArrayList<>();
                length = 0;
                es.submit(new StoreToHdfs(id++, temp));
            }
        }
    }


    class StoreToHdfs implements Runnable{

        private List<String> lines;

        private int id;

        public StoreToHdfs(int id, List<String> lines){
            this.id = id;
            this.lines = lines;
        }


        @Override
        public void run() {
            System.out.println(String.format("StoreToHdfs-%d task is run, lines count is %d", id, lines.size()));
            String fileName = "music-listen-" + id;
            hdfsDao.createFile(fileName);
            OutputStreamWriter osw = hdfsDao.createOutputStreamWriter(fileName);
            hdfsDao.writeToHDFS(lines, osw);
            hdfsDao.closeWriter(osw);
            System.out.println(String.format("StoreToHdfs-%d task is end", id));
        }
    }
}
