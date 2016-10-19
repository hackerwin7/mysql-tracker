import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by hp on 14-12-8.
 */
public class KafkaProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");//msg is string
        props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        Random rnd = new Random();
        long events = 1;
        for(int i=0;i<events;i++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com." + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
            producer.send(data);
        }
        producer.close();
    }

    class SimplePartitioner implements Partitioner {
        public SimplePartitioner(VerifiableProperties props) {

        }

        public int partition(Object key, int partitionNum) {
            int partition = 0;
            String stringKey = (String) key;
            int offset = stringKey.lastIndexOf('.');
            if(offset > 0) {
                partition = Integer.parseInt(stringKey.substring(offset+1)) % partitionNum;
            }
            return partition;
        }
    }
}
