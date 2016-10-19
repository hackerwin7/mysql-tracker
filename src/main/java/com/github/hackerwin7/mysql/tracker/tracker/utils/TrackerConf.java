package com.github.hackerwin7.mysql.tracker.tracker.utils;

import com.github.hackerwin7.jd.lib.utils.SiteConfService;
import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.tracker.protocol.json.ConfigJson;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.ho.yaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hp on 14-12-12.
 */
public class TrackerConf {
    public Logger logger = LoggerFactory.getLogger(TrackerConf.class);

    //mysql conf
    public String username = "canal";
    public String password = "canal";
    public String address = "127.0.0.1";
    public int myPort = 3306;
    public long slaveId = 15879;
    //kafka conf
    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int kafkaPort = 9092;
    public static String zkKafka = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String keySerializer = "kafka.serializer.StringEncoder";
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public static String kafkaCompression = "snappy";
    public static String acks = "-1";
    public static String topic = "test";//queue topic
    public static int partition = 0;
    public static String strSeeds = "127.0.0.1";//"172.17.36.53,172.17.36.54,172.17.36.55";
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    public static final int KAFKA_SEND_RETRY = 3;
    public static final int KAFKA_CONN_RETRY = 1;
    public static final long KAFKA_RETRY_INTERNAL = 3000;
    public static final long KAFKA_SEND_COMPRESS_BATCH_BYTES = 1000 * 1024;//compressed batch size must < 1MB
    public static final long KAFKA_SEND_DEFAULT_BATCH_BYTES = 1024 * 1024;
    public static final long KAFKA_SEND_UNCOMPRESS_BATCH_BYTES = 10 * 1024 * 1024;//10 MB
    public long sendBytes = KAFKA_SEND_DEFAULT_BATCH_BYTES;//unit is Bytes, 1024 * 1024 is 1MB, 1MB must send
    public long sendTimeInterval = 5000;//5 second must send
    //zk conf
    public String zkServers = "127.0.0.1:2181";//"48:2181,19:2181,50:2181"
    public int timeout = 100000;
    public String rootPath = "/checkpoint";
    public String persisPath = rootPath + "/persistence";
    public String minutePath = rootPath + "/minutes";
    //tracker conf
    public int batchsize = 50000;// default is 10000
    public int queuesize = 80000;//default is 50000
    public int sumBatch = 5 * batchsize;
    public int timeInterval = 1;
    public int reInterval = 3;
    public String filterRegex = ".*\\..*";
    public int minsec = 60;
    public int heartsec = 1 * 60;//10
    public int retrys = 3;//if we retry 100 connect or send failed too, we will reload the job //interval time
    public double mbUnit = 1024.0 * 1024.0;//unit for 1MB, do not edit it
    public String jobId = "mysql-tracker";
    public int spacesize = 8;//8 MB, send batch size
    public int monitorsec = 60;//1 minute
    public static int FILTER_NUM_LOGGER = 500;
    public static int CONTINUOUS_ZERO_NUM = 20;
    public static long SLEEPING_RUNNING = 3000;
    public static int CP_RETRY_COUNT = 3;
    public static long TIMER_TASK_DELAY = 10 * 1000;
    public static long CONFIRM_INTERVAL = 60 * 1000;//60 seconds
    public static long HEARTBEAT_INTERVAL = 60 * 1000;//60 seconds
    //phenix monitor
    public String phKaBrokerList = "localhost:9092";
    public int phKaPort = 9092;
    public String phKaZk = "localhost:2181";
    public String phKaSeria = "kafka.serializer.DefaultEncoder";
    public String phKaKeySeria = "kafka.serializer.StringEncoder";
    public String phKaParti = "kafka.producer.DefaultPartitioner";
    public String phKaAcks = "1";
    public String phKaTopic = "test1";
    public int phKaPartition = 0;
    //charset mysql tracker
    public Charset charset = Charset.forName("UTF-8");
    //filter same to parser
    public static Map<String, String> filterMap = new HashMap<String, String>();
    //position
    public String logfile = null;
    public long offset = -1;
    public long batchId = 0;
    public long inId = 0;
    public String CLASS_PREFIX = "classpath:";
    //constants
    private static String confPath = "tracker.properties";
    //url config file
    public static final String LOAD_FILE_CONF = "input_config.yaml";

    public String toString() {
        StringBuilder cons = new StringBuilder();
        cons.append("================================> load conf = \n{\n");
        cons.append("jobId = " + jobId).append("\n");
        cons.append("mysql username = " + username).append("\n");
        cons.append("mysql password = " + password).append("\n");
        cons.append("mysql address = " + address).append("\n");
        cons.append("mysql port = " + myPort).append("\n");
        cons.append("mysql slave id = " + slaveId).append("\n");
        cons.append("mysql charset = " + charset);
        cons.append("kafka zk = " + zkKafka).append("\n");
        cons.append("kafka broker list = " + brokerList).append("\n");
        cons.append("kafka port = " + kafkaPort).append("\n");
        cons.append("kafka ack = " + acks).append("\n");
        cons.append("kafka topic = " + topic).append("\n");
        cons.append("offset zk = " + zkServers).append("\n");
        cons.append("monitor kafka zk = " + phKaZk).append("\n");
        cons.append("monitor kafka broker list = " + phKaBrokerList).append("\n");
        cons.append("monitor kafka topic = " + phKaTopic).append("\n");
        cons.append("filter db tb map = " + filterMap).append("\n");
        cons.append("}");
        return cons.toString();
    }


    public void initConfLocal() {
        brokerSeeds.add("127.0.0.1");
    }

    public void initConfStatic() {
        username = "canal";
        password = "canal";
        address = "192.168.144.109";
        myPort = 3306;
        slaveId = 135468L;
        zkServers = "192.168.144.110:2181,192.168.144.111:2181,192.168.144.112:2181";
    }

    public void initConfFile() throws Exception {
        clear();
        String cnf = System.getProperty("tracker.conf", "classpath:tracker.properties");
        logger.info("load file : " + cnf);
        InputStream in = null;
        if(cnf.startsWith(CLASS_PREFIX)) {
            cnf = StringUtils.substringAfter(cnf, CLASS_PREFIX);
            in = TrackerConf.class.getClassLoader().getResourceAsStream(cnf);
        } else {
            in = new FileInputStream(cnf);
        }
        Properties pro = new Properties();
        pro.load(in);
        //load the parameter
        jobId = pro.getProperty("job.name");
        charset = Charset.forName(pro.getProperty("job.charset"));
        address = pro.getProperty("mysql.address");
        myPort = Integer.valueOf(pro.getProperty("mysql.port"));
        username = pro.getProperty("mysql.usr");
        password = pro.getProperty("mysql.psd");
        slaveId = Long.valueOf(pro.getProperty("mysql.slaveId"));
        String dataKafkaZk = pro.getProperty("kafka.data.zkserver") + pro.getProperty("kafka.data.zkroot");
        KafkaConf dataCnf = new KafkaConf();
        dataCnf.loadZk(dataKafkaZk);
        brokerList = dataCnf.brokerList;
        acks = pro.getProperty("kafka.acks");
        topic = pro.getProperty("kafka.data.topic.tracker");
        zkServers = pro.getProperty("zookeeper.servers");
        String monitorKafkaZk = pro.getProperty("kafka.monitor.zkserver") + pro.getProperty("kafka.monitor.zkroot");
        KafkaConf monitorCnf = new KafkaConf();
        monitorCnf.loadZk(monitorKafkaZk);
        phKaBrokerList = monitorCnf.brokerList;
        phKaTopic = pro.getProperty("kafka.monitor.topic");
        in.close();
    }

    public void initConfJSON() {
        ConfigJson jcnf = new ConfigJson(jobId, "magpie.address");
        JSONObject root = jcnf.getJson();
        //parser the json
        if(root != null) {
            JSONObject data = root.getJSONObject("info").getJSONObject("content");
            username = data.getString("username");
            password = data.getString("password");
            address = data.getString("address");
            myPort = Integer.valueOf(data.getString("myPort"));
            slaveId = Long.valueOf(data.getString("slaveId"));
            brokerList = data.getString("brokerList");
            kafkaPort = Integer.valueOf(data.getString("kafkaPort"));
            zkKafka = data.getString("zkKafka");
            topic = data.getString("topic");
            strSeeds = data.getString("strSeeds");
            zkServers = data.getString("zkServers");
            filterRegex = data.getString("filterRegex");
        }
    }

    public void initConfOnlineJSON() throws Exception {
        clear();
        ConfigJson jcnf = new ConfigJson(jobId, "release.address");
        JSONObject root = jcnf.getJson();
        //parse the json
        if (root != null) {
            int _code = root.getInt("_code");
            if (_code != 0) {
                String errMsg = root.getString("errorMessage");
                throw new Exception(errMsg);
            }
            JSONObject data = root.getJSONObject("data");
            //mysql simple load
            username = data.getString("source_user");
            password = data.getString("source_password");
            address = data.getString("source_host");
            myPort = Integer.valueOf(data.getString("source_port"));
            slaveId = Long.valueOf(data.getString("slaveId"));
            charset = Charset.forName(data.getString("source_charset"));
            //get kafka parameter from zk
            String dataKafkaZk = data.getString("kafka_zkserver") + data.getString("kafka_zkroot");
            zkKafka = dataKafkaZk;
            KafkaConf dataCnf = new KafkaConf();
            dataCnf.loadZk(dataKafkaZk);
            brokerList = dataCnf.brokerList;
            //kafka simple load json
            acks = data.getString("kafka_acks");
            topic = data.getString("tracker_log_topic");
            //load own zk
            zkServers = data.getString("offset_zkserver");
            //get kafka monitor parameter from zk
            String monitorKafkaZk = data.getString("monitor_server") + data.getString("monitor_zkroot");
            phKaZk = monitorKafkaZk;
            KafkaConf monitorCnf = new KafkaConf();
            monitorCnf.loadZk(monitorKafkaZk);
            phKaBrokerList = monitorCnf.brokerList;
            //kafka simple loadjson
            phKaTopic = data.getString("monitor_topic");
            //jobId
            jobId = data.getString("job_id");
            //filter load
            if (data.containsKey("db_tab_meta")) {
                JSONArray jf = data.getJSONArray("db_tab_meta");
                for (int i = 0; i <= jf.size() - 1; i++) {
                    JSONObject jdata = jf.getJSONObject(i);
                    String dbname = jdata.getString("dbname");
                    String tbname = jdata.getString("tablename");
                    String key = dbname + "." + tbname;
                    String value = tbname;
                    filterMap.put(key, value);
                }
            }
            //load position
            if (data.containsKey("position-logfile")) {
                logfile = data.getString("position-logfile");
            }
            if (data.containsKey("position-offset")) {
                offset = Long.valueOf(data.getString("position-offset"));
            }
            if (data.containsKey("position-bid")) {
                batchId = Long.valueOf(data.getString("position-bid"));
            }
            if (data.containsKey("position-iid")) {
                inId = Long.valueOf(data.getString("position-iid"));
            }
        }

        //related check
        if (!StringUtils.equalsIgnoreCase(kafkaCompression, "none"))
            sendBytes = KAFKA_SEND_COMPRESS_BATCH_BYTES;
        else
            sendBytes = KAFKA_SEND_UNCOMPRESS_BATCH_BYTES;
    }

    /**
     * read the config from the site config
     * @throws Exception
     */
    public void initConfigFromSiteConfig() throws Exception {
        clear();
        SiteConfService scs = new SiteConfService();
        logger.error("loading config");
        String val = scs.read(jobId);
        logger.error("loaded config");
        //parse the json
        logger.error("parsing json.......");
        JSONObject data = JSONObject.fromObject(val);
        //mysql simple load
        username = data.getString("source_user");
        password = data.getString("source_password");
        address = data.getString("source_host");
        myPort = Integer.valueOf(data.getString("source_port"));
        slaveId = Long.valueOf(data.getString("slaveId"));
        charset = Charset.forName(data.getString("source_charset"));
        //get kafka parameter from zk
        String dataKafkaZk = data.getString("kafka_zkserver") + data.getString("kafka_zkroot");
        zkKafka = dataKafkaZk;
        KafkaConf dataCnf = new KafkaConf();
        dataCnf.loadZk(dataKafkaZk);
        brokerList = dataCnf.brokerList;
        //kafka simple load json
        acks = data.getString("kafka_acks");
        topic = data.getString("tracker_log_topic");
        //load own zk
        zkServers = data.getString("offset_zkserver");
        //get kafka monitor parameter from zk
        String monitorKafkaZk = data.getString("monitor_server") + data.getString("monitor_zkroot");
        phKaZk = monitorKafkaZk;
        KafkaConf monitorCnf = new KafkaConf();
        monitorCnf.loadZk(monitorKafkaZk);
        phKaBrokerList = monitorCnf.brokerList;
        //kafka simple loadjson
        phKaTopic = data.getString("monitor_topic");
        //jobId
        jobId = data.getString("job_id");
        //filter load
        if(data.containsKey("db_tab_meta")) {
            JSONArray jf = data.getJSONArray("db_tab_meta");
            for (int i = 0; i <= jf.size() - 1; i++) {
                JSONObject jdata = jf.getJSONObject(i);
                String dbname = jdata.getString("dbname");
                String tbname = jdata.getString("tablename");
                String key = dbname + "." + tbname;
                String value = tbname;
                filterMap.put(key, value);
            }
        }
        //load position
        if(data.containsKey("position-logfile")) {
            logfile = data.getString("position-logfile");
        }
        if(data.containsKey("position-offset")) {
            offset = Long.valueOf(data.getString("position-offset"));
        }
        if(data.containsKey("position-bid")) {
            batchId = Long.valueOf(data.getString("position-bid"));
        }
        if(data.containsKey("position-iid")) {
            inId = Long.valueOf(data.getString("position-iid"));
        }

        kafkaCompression = "none";

        //related check
        if (!StringUtils.equalsIgnoreCase(kafkaCompression, "none"))
            sendBytes = KAFKA_SEND_COMPRESS_BATCH_BYTES;
        else
            sendBytes = KAFKA_SEND_UNCOMPRESS_BATCH_BYTES;
    }

    //clear the conf info
    public void clear() {
        brokerSeeds.clear();
        filterMap.clear();
    }
}
