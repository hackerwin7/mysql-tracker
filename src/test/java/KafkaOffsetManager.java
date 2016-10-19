import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 14-12-11.
 */
public class KafkaOffsetManager {

    //get consumer last offset
    public static long getLastOffset(SimpleConsumer consumer, String groupId, String topic, int partition) {
        List<TopicAndPartition> requestInfo = new ArrayList<TopicAndPartition>();
        TopicAndPartition tp = new TopicAndPartition(topic, partition);
        requestInfo.add(tp);
        OffsetFetchRequest request = new OffsetFetchRequest(groupId, requestInfo, kafka.api.OffsetRequest.CurrentVersion(), 1, "test");
        OffsetFetchResponse response = consumer.fetchOffsets(request);
        OffsetMetadataAndError meta =  response.offsets().get(tp);
        return meta.offset();
    }

    //get producer last offset
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whitchTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
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

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 9092;
        int timeout = 10000;
        int buffersize = 64 * 1024;
        String clientId = "get-last-offset";
        String groupId = "test";
        String topic = "test";
        int partition = 0;
        SimpleConsumer consumer = new SimpleConsumer(host, port, timeout, buffersize, clientId);
        long offset = getLastOffset(consumer, groupId, topic, partition);
        System.out.println("offset : " + offset);
        consumer.close();

        SimpleConsumer consumerTime = new SimpleConsumer(host, port, timeout, buffersize, clientId);
        offset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientId);
        System.out.println("offset : " + offset);
        consumer.close();
    }

}
