import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by hp on 14-12-8.
 */
public class KafkaConsumerSimple {

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {
        Properties prop = new Properties();
        prop.put("auto.offset.reset","smallest");
        prop.put("zookeeper.connect","127.0.0.1:2181");
        prop.put("zookeeper.session.timeout.ms", "400");
        prop.put("zookeeper.sync.time.ms", "200");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("group.id", "test");

        ConsumerConfig config = new ConsumerConfig(prop);
        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put("test", 1);//topic name and partition count
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStream = javaConsumerConnector.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStream.get("test");
        for(KafkaStream<byte[], byte[]> stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
//                MessageAndMetadata<byte[], byte[]> meta = it.next();//why these is not same
//                System.out.println("Message:" + new String(meta.message()));
//                System.out.println("partition:" + meta.partition());
//                System.out.println("offset:" + meta.offset());
                  System.out.println("Msg : " + new String(it.next().message()));
            }
        }
        javaConsumerConnector.shutdown();
    }
}
