package com.github.hackerwin7.mysql.tracker.kafka.utils;

import net.sf.json.JSONObject;
import com.github.hackerwin7.mysql.tracker.zk.client.ZkExecutor;
import com.github.hackerwin7.mysql.tracker.zk.utils.ZkConf;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaConf {

    public String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public int port = 9092;
    public String zk = "localhost:2181";
    public String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public String keySerializer = "kafka.serializer.StringEncoder";//default is message's byte[]
    public String partitioner = "kafka.producer.DefaultPartitioner";
    public String compression = "none";
    public String acks = "1";
    public String sendBufferSize = String.valueOf(1024 * 1024);//1MB
    public String topic;//queue topic
    public int partition = 0;
    public List<String> topics = new ArrayList<String>();//distribute the multiple topic
    public List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    public List<Integer> portList = new ArrayList<Integer>();//9092 9093 9094
    public int readBufferSize = 1 * 1024 * 1024;//1 MB
    public String clientName = "cc456687IUGHG";

    //load the zkPos to find the bokerList and port zkPos : 172.17.36.60:2181/kafka
    public void loadZk(String zkPos) throws Exception{
        if(zkPos == null) throw new Exception("zk path is null");
        String[] ss = zkPos.split("/");
        String zkServer = "";
        String zkPath = "";
        for(int i = 0; i<= ss.length - 1; i++) {
            if(i == 0) {
                zkServer = ss[i];
            } else {
                zkPath += ("/" + ss[i]);
            }
        }
        zkPath += ("/brokers/ids");
        ZkConf zcnf = new ZkConf();
        zcnf.zkServers = zkServer;
        ZkExecutor zkexe = new ZkExecutor(zcnf);
        zkexe.connect();
        List<String> ids = zkexe.getChildren(zkPath);
        brokerList = "";
        brokerSeeds.clear();
        portList.clear();
        for(String brokerNode : ids) {
            String zkNodeJson = zkexe.get(zkPath + "/" + brokerNode);
            if(zkNodeJson == null) continue;
            JSONObject jo = JSONObject.fromObject(zkNodeJson);
            String host = jo.getString("host");
            int port = jo.getInt("port");
            brokerSeeds.add(host);
            portList.add(port);
            brokerList += (host + ":" + port + ",");
        }
        brokerList = brokerList.substring(0, brokerList.lastIndexOf(","));
        zkexe.close();
    }

}
