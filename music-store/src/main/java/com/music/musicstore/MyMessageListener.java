package com.music.musicstore;

import org.springframework.kafka.listener.BatchMessageListener;

/**
 * <p>
 * description:
 * </p>
 * Created on 2018/4/20 15:12
 *
 * @author leiguang
 */
public class MyMessageListener<K, V> implements BatchMessageListener {
    @Override
    public void onMessage(Object o) {

    }
}
