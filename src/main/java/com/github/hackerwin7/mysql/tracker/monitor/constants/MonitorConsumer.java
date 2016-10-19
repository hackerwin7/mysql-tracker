package com.github.hackerwin7.mysql.tracker.monitor.constants;

import com.github.hackerwin7.mysql.tracker.kafka.driver.consumer.KafkaReceiver;
import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaMetaMsg;
import com.github.hackerwin7.mysql.tracker.protocol.avro.EventEntryAvro;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 15-1-14.
 */
public class MonitorConsumer {

    private Logger logger = LoggerFactory.getLogger(MonitorConsumer.class);

    private KafkaConf kcnf;
    private KafkaReceiver kr;
    public boolean running = true;
    private List<byte[]> msgList = new ArrayList<byte[]>();

    public MonitorConsumer(KafkaConf kc) {
        kcnf = kc;
        kr = new KafkaReceiver(kcnf);
    }

    public MonitorConsumer() {
        kcnf = new KafkaConf();
        loadStatic();
        kr = new KafkaReceiver(kcnf);
    }

    private void loadStatic() {
        kcnf.brokerSeeds.add("172.17.36.53");
        kcnf.brokerSeeds.add("172.17.36.54");
        kcnf.brokerSeeds.add("172.17.36.55");
        kcnf.port = 9092;
        kcnf.portList.add(9092);
        kcnf.portList.add(9092);
        kcnf.portList.add(9092);
        kcnf.partition = 0;
        kcnf.topic = "mysql_monitor";
    }

    public void dump() throws Exception {
        //thread start dumping
        Thread tdump = new Thread(new Runnable() {
            @Override
            public void run() {
                kr.run();
            }
        });
        tdump.start();
        while (running) {
            while (!kr.msgQueue.isEmpty()) {
                KafkaMetaMsg kmsg = kr.msgQueue.take();
                msgList.add(kmsg.msg);
            }
            for(byte[] value : msgList) {
                String jsonStr = new String(value);
                logger.info("monitor json string:" + jsonStr);
            }
            msgList.clear();
        }
    }

    private String getColVal(Map<CharSequence, CharSequence> cv) {
        String constr = "";
        if(cv != null) {
            Iterator iter = cv.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                constr += ("[" + key.toString() + "," + value.toString() + "]");
            }
        }
        return constr;
    }

    private EventEntryAvro getAvroFromBytes(byte[] value) {
        SpecificDatumReader<EventEntryAvro> reader = new SpecificDatumReader<EventEntryAvro>(EventEntryAvro.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value,null);
        EventEntryAvro avro = null;
        try {
            avro = reader.read(null,decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avro;
    }

    public static void main(String[] args) throws Exception {
        MonitorConsumer mc = new MonitorConsumer();
        mc.dump();
    }

}
