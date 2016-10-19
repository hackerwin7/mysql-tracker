package com.github.hackerwin7.mysql.tracker.monitor;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by hp on 14-9-26.
 */
public class MonitorToKafkaProducer {

    private String brokerList = "localhost:9092";
    private String serializerClass = "kafka.serializer.StringEncoder";
    private String partitionerClass = "SimplePartitioner";
    private String acks = "1";
    private String topic = "mysql_tracker_parser";

    private ProducerConfig config = null;
    private Properties props = null;
    private Producer<String, String> producer = null;

    public MonitorToKafkaProducer() {

    }


    public MonitorToKafkaProducer(String broker, String serializer, String partitioner, String acks) {
        brokerList = broker;
        serializerClass = serializer;
        partitionerClass = partitioner;
        this.acks = acks;
    }

    public void open() throws Exception {
        props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", serializerClass);
        props.put("partitioner.class", partitionerClass);
        props.put("request.required.acks", acks);
        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void send(String key ,String msg) throws Exception {
        if(config == null) {
            throw new NullPointerException("process open function first!!!");
        }
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, key, msg);
        producer.send(message);
    }

    public void close() throws Exception {
        producer.close();
    }

}
