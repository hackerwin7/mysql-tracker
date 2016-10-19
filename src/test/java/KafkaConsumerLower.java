import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;


import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by hp on 14-12-8.
 */
public class KafkaConsumerLower {

    private List<String> replicaBrokers = new ArrayList<String>();

    private PartitionMetadata findLeader(List<String> brokers, int port, String topic, int partition) {
        PartitionMetadata returnData = null;
        loop:
        for (String seed : brokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with broker [" + seed + "] to find leader for [" +
                        topic + "," + partition + "] Reason: " + e);
                e.printStackTrace();
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnData != null) {
            replicaBrokers.clear();
            for (Broker replica : returnData.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }
        return returnData;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whitchTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, kafka.api.PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, kafka.api.PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new kafka.api.PartitionOffsetRequestInfo(whitchTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if(response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public long getLastOffset(SimpleConsumer consumer, String groupId, String topic, int partition) {
        List<TopicAndPartition> requestInfo = new ArrayList<TopicAndPartition>();
        TopicAndPartition tp = new TopicAndPartition(topic, partition);
        requestInfo.add(tp);
        OffsetFetchRequest request = new OffsetFetchRequest(groupId, requestInfo, kafka.api.OffsetRequest.CurrentVersion(), 1, "test");
        OffsetFetchResponse response = consumer.fetchOffsets(request);
        OffsetMetadataAndError meta =  response.offsets().get(tp);
        return meta.offset();
    }

    private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
        for(int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
            if(metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if(goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    public void run(long maxReads, String topic, int partition, List<String> brokers, int port) throws Exception {
        PartitionMetadata metadata = findLeader(brokers, port, topic, partition);
        if(metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Existing");
            return;
        }
        if(metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Existing");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition;
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
        System.out.println("init offset : " + readOffset);
        int numErrors = 0;
        while(true) {
            if(consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }
            kafka.api.FetchRequest req = new kafka.api.FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
            if(fetchResponse.hasError()) {
                numErrors++;
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if(numErrors > 5) break;
                if(code == ErrorMapping.OffsetOutOfRangeCode()) {
                    readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;
            long numRead = 0;
            for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if(currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                numRead++;
                maxReads--;
            }
            if(numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        if(consumer != null) consumer.close();
    }

    public static void main(String[] args) {
        KafkaConsumerLower example = new KafkaConsumerLower();
        long maxReads = 1000;
        String topic = "test";
        int partition = 0;
        List<String> seeds = new ArrayList<String>();
        seeds.add("127.0.0.1");
        int port = 9092;
        try {
            example.run(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }
    }
}
