import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hp on 14-12-8.
 */
public class KafkaConsumerTest {

    private final String topic;
    private final ConsumerConnector consumer;
    private ExecutorService executor;

    public KafkaConsumerTest(String zk, String groupId, String sTopic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zk, groupId));
        topic = sTopic;
    }

    public void shutdown() {
        if(consumer != null) consumer.shutdown();
        if(executor != null) executor.shutdown();
    }

    public void run(int numThread) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executor = Executors.newFixedThreadPool(numThread);
        int threadNum = 0;
        for(final KafkaStream stream : streams) {
            executor.submit(new ConsumerRunnable(stream, threadNum));
            threadNum++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String zk, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zk);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    class ConsumerRunnable implements Runnable {
        private KafkaStream mStream;
        private int mThread;
        public ConsumerRunnable(KafkaStream stream, int thread) {
            mStream = stream;
            mThread = thread;
        }

        public void run () {
            ConsumerIterator<byte[], byte[]> it = mStream.iterator();
            while(it.hasNext()) {
                System.out.println("Thread " + mThread + ": " + new String(it.next().message()));
            }
            System.out.println("shutting down thread : " + mThread);
        }
    }

    public static void main(String[] args) {
        String zk = "127.0.0.1:2181";
        String groupId = "test-consumer-group";
        String topic = "page_visits";
        int threads = 1;
        KafkaConsumerTest example = new KafkaConsumerTest(zk, groupId, topic);
        example.run(threads);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        example.shutdown();
    }
}
