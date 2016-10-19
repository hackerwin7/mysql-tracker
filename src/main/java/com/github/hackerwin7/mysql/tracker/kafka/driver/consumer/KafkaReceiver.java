package com.github.hackerwin7.mysql.tracker.kafka.driver.consumer;


import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaMetaMsg;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-12-12.
 */

/**
 *  while(isFetch) {
 *      request req;
 *      response rep = consumer.fetch(req);
 *      getMessage(rep);
 *  }
 * */
public class KafkaReceiver extends Thread {

    private Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);
    private KafkaConf conf;
    private List<String> replicaBrokers = new ArrayList<String>();
    private List<Integer> replicaPorts = new ArrayList<Integer>();
    public static int retry = 3;
    private int MAXLEN = 10000;
    private SimpleConsumer consumer;
    public BlockingQueue<KafkaMetaMsg> msgQueue = new LinkedBlockingQueue<KafkaMetaMsg>(MAXLEN);//outer interface, outer read data from this queue.
    public boolean isFetch = true;

    public KafkaReceiver(KafkaConf cnf) {
        conf = cnf;
    }

    public KafkaReceiver(KafkaConf cnf, int qSize) {
        conf = cnf;
        MAXLEN = qSize;
    }



    public PartitionMetadata findLeader(List<String> brokers, int port, String topic, int partition) {
        PartitionMetadata returnData = null;
        loop:
        for (String broker : brokers) {
            SimpleConsumer consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, "leader");
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            TopicMetadataResponse rep = consumer.send(req);
            List<TopicMetadata> topicMetadatas = rep.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadatas) {
                for (PartitionMetadata part : topicMetadata.partitionsMetadata()) {
                    if(part.partitionId() == partition) {
                        returnData = part;
                        break loop;
                    }
                }
            }
        }
        if(returnData != null) {
            replicaBrokers.clear();
            for (Broker broker : returnData.replicas()) {
                replicaBrokers.add(broker.host());
            }
        }
        return returnData;
    }

    //two List length must be equal
    public PartitionMetadata findLeader(List<String> brokers, List<Integer> ports, String topic, int partition) {
        PartitionMetadata returnData = null;
        loop:
        for (int i = 0; i <= brokers.size() - 1; i++) {
            SimpleConsumer consumer = new SimpleConsumer(brokers.get(i), ports.get(i), 100000, 64 * 1024, "leader");
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            TopicMetadataResponse rep = consumer.send(req);
            List<TopicMetadata> topicMetadatas = rep.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadatas) {
                for (PartitionMetadata part : topicMetadata.partitionsMetadata()) {
                    if(part.partitionId() == partition) {
                        returnData = part;
                        break loop;
                    }
                }
            }
        }
        if(returnData != null) {
            replicaBrokers.clear();
            replicaPorts.clear();
            for (Broker broker : returnData.replicas()) {
                replicaBrokers.add(broker.host());
                replicaPorts.add(broker.port());
            }
        }
        return returnData;
    }

    public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whitchTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whitchTime, 1));
        OffsetRequest req = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse rep = consumer.getOffsetsBefore(req);
        if(rep.hasError()) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + rep.errorCode(topic, partition));
            return -1;
        }
        long[] offsets = rep.offsets(topic, partition);
        return offsets[0];
    }

    public String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
        for(int i = 0; i < retry; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
            if(metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if(oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if(goToSleep) {
                delay(1);
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    public String findNewLeader(String oldLeader, String topic, int partition) throws Exception {
        for(int i = 0; i < retry; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, replicaPorts, topic, partition);
            if(metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if(oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if(goToSleep) {
                delay(1);
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    public void run() {
        PartitionMetadata metadata = findLeader(conf.brokerSeeds, conf.portList, conf.topic, conf.partition);
        if(metadata == null) {
            logger.error("Can't find metadata for Topic and Partition. Existing");
            return;
        }
        if(metadata.leader() == null) {
            logger.error("Can't find Leader for Topic and Partition. Existing");
            return;
        }
        String leadBroker = metadata.leader().host();
        int leadPort = metadata.leader().port();
        String clientName = "client_" + conf.topic + conf.partition;
        consumer = new SimpleConsumer(leadBroker, leadPort, 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), clientName);
        int numErr = 0;
        while (isFetch) {
            if(consumer == null) {
                consumer = new SimpleConsumer(leadBroker, leadPort, 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(conf.topic, conf.partition, readOffset, conf.readBufferSize)
                    .build();
            FetchResponse rep = consumer.fetch(req);
            if(rep.hasError()) {
                numErr++;
                short code = rep.errorCode(conf.topic, conf.partition);
                logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if(numErr > 5) {
                    logger.error("5 errors occurred existing the fetching");
                    break;
                }
                if(code == ErrorMapping.OffsetOutOfRangeCode()) {
                    readOffset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                try {
                    leadBroker = findNewLeader(leadBroker, conf.topic, conf.partition);
                } catch (Exception e) {
                    logger.error("find lead broker failed");
                    e.printStackTrace();
                    break;
                }
                continue;
            }
            numErr = 0;
            long numRead=0;
            for(MessageAndOffset messageAndOffset : rep.messageSet(conf.topic, conf.partition)) {
                long currentOffset = messageAndOffset.offset();
                if(currentOffset < readOffset) {
                    logger.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                long offset = messageAndOffset.offset();
                KafkaMetaMsg metaMsg = new KafkaMetaMsg(bytes, offset);
                try {
                    msgQueue.put(metaMsg);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                }
                numRead++;
            }
            if(numRead == 0) {
                delay(1);//block
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

    public boolean isConnected() {
        SimpleConsumer hconsumer = null;
        try {
            for (int i = 0; i <= conf.brokerSeeds.size() - 1; i++) {
                hconsumer = new SimpleConsumer(conf.brokerSeeds.get(i), conf.portList.get(i), 100000, 64 * 1024, "heartBeat");
                List<String> topics = Collections.singletonList(conf.topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse rep = hconsumer.send(req);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if(hconsumer != null) hconsumer.close();
        }
        return true;
    }
}
