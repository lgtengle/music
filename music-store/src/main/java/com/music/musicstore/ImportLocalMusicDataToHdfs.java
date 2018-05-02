package com.music.musicstore;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * description: 导入本地的音乐数据到hdfs中
 * </p>
 * Created on 2018/5/2 14:57
 *
 * @author leiguang
 */
//@Component
public class ImportLocalMusicDataToHdfs {

    @Autowired
    HdfsDao hdfsDao;

    @PostConstruct
    public void import1() throws IOException {
        hdfsDao.createFile("file1");
        List<String> result = new ArrayList<>();
        Pattern p = Pattern.compile("^(\\d+)([\\u4E00-\\u9FA5]+)$");
        //小插曲，音乐数据有500M，刚开始用common-io读取整个文件时，发现程序执行的很慢，最后查看GC情况，
        //发现Old Gen被几乎占满了，而且在频繁的GC，导致了程序停顿时间很长，所以执行的很慢，所以还在自己在来造轮子了，速度快了不少...
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\data\\music\\虾米300万试听记录.json"), "UTF-8"));
        String s = null;
        while ((s = br.readLine()) != null) {
            try {
                JSONObject jsonObject = new JSONObject(s);
                String song = jsonObject.getString("song");
                String artist = jsonObject.getString("artist");
                String user = jsonObject.getString("user");
                String platform = jsonObject.getString("platform");
                String time = jsonObject.getString("time");
                Matcher m = p.matcher(time);
                if (m.find()){
                    int t = Integer.parseInt(m.group(1));
                    String typeName = m.group(2);
                    String scratch_time = jsonObject.getString("scratch_time");
                    Calendar instance = Calendar.getInstance();
                    try {
                        instance.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(scratch_time));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    int type = 0;
                    if (typeName.contains("秒")){
                        type = Calendar.SECOND;
                    }else if (typeName.contains("分钟")){
                        type = Calendar.MINUTE;
                    }else if (typeName.contains("小时")){
                        type = Calendar.HOUR_OF_DAY;
                    }else if (typeName.contains("天")){
                        type = Calendar.DAY_OF_MONTH;
                    }
                    instance.set(type, -t);
                    time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(instance.getTime());
                }
                String line = new StringBuilder().append(song)
                        .append("\t")
                        .append(artist)
                        .append("\t")
                        .append(user)
                        .append("\t")
                        .append(platform)
                        .append("\t")
                        .append(time).toString();
                result.add(line);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        br.close();

        OutputStreamWriter osw = hdfsDao.createOutputStreamWriter("file1");
        hdfsDao.writeToHDFS(result, osw);
        hdfsDao.closeWriter(osw);
        hdfsDao.close();
    }



}
