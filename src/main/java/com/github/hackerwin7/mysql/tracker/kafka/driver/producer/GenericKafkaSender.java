package com.github.hackerwin7.mysql.tracker.kafka.driver.producer;

import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaConf;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Created by hp on 15-1-8.
 */
public class GenericKafkaSender<T1, T2> {

    private Logger logger = LoggerFactory.getLogger(GenericKafkaSender.class);

    private KafkaConf conf;
    private Producer<T1, T2> producer;
    private int retrys = 100;

    public GenericKafkaSender(KafkaConf cf) {
        conf = cf;
    }

    public void connect() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("key.serializer.class", conf.keySerializer);
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        ProducerConfig pConfig = new ProducerConfig(prop);
        producer = new Producer<T1, T2>(pConfig);
    }

    public void sendKeyMsg(List<KeyedMessage<T1, T2>> keyMsgs) {
        blockSend(keyMsgs);
    }
    public void sendKeyMsg(KeyedMessage<T1, T2> km) {
        blockSend(km);
    }

    public void blockSend(List<KeyedMessage<T1, T2>> keyMsgs) {
        boolean isAck = false;
        int retryKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                logger.warn("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsgs);
                isAck = true;
            } catch (Exception e) {
                logger.warn("retrying sending... Exception:" + e.getMessage());
                delay(3);
            }
        }
    }

    public void blockSend(KeyedMessage<T1, T2> keyMsg) {
        boolean isAck = false;
        int retryKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                logger.warn("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsg);
                isAck = true;
            } catch (Exception e) {
                logger.warn("retrying sending... Exception:" + e.getMessage());
                delay(3);
            }
        }
    }

    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if(producer != null) producer.close();
    }

    public void reconnect() {
        close();
        connect();
    }

    public boolean isConnected() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("key.serializer.class", conf.keySerializer);
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        prop.put("send.buffer.bytes",  conf.sendBufferSize);
        ProducerConfig pConfig = new ProducerConfig(prop);
        Producer<String, byte[]> heartPro = null;
        try {
            heartPro = new Producer<String, byte[]>(pConfig);
            if(heartPro != null) heartPro.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}
