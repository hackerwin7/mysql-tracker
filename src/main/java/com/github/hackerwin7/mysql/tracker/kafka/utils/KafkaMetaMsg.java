package com.github.hackerwin7.mysql.tracker.kafka.utils;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaMetaMsg {

    public byte[] msg;
    public long offset;

    public KafkaMetaMsg(byte[] bytes, long pos) {
        msg = bytes;
        offset = pos;
    }

}
