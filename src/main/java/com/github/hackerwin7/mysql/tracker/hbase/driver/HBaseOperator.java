package com.github.hackerwin7.mysql.tracker.hbase.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 14-9-17.
 */
public class HBaseOperator {

    //hadoop config
    private Configuration conf;

    //kinds of htale
    private HTable hEventWriter;
    private HTable hEventReader;
    private HTable hEntryWriter;
    private HTable hEntryReader;
    private HTable hCheckpointWriter;
    private HTable hCheckpointReader;

    //entry table name
    private String eventBytesSchemaName = "mysql_event";

    //checkpoint table name
    private String checkpointSchemaName = "mysql_checkpoint";

    //minute table name
    private String entryDataSchemaName = "mysql_entry";

    //job id
    private String mysqlId = "127.0.0.1:3306";

    //public global single rowkey for tracker and parser and per minute row key = single parser + time
    public  String trackerRowKey = "jd-MysqlTracker";
    public  String parserRowKey = "jd-MysqlParser";
    public  String binlogXidCol = "BinlogXid";
    public  String eventXidCol = "EventXidRowKey";
    public  String eventRowCol = "EventRowKey";
    public  String entryRowCol = "EntryRowKey";//is checkpoint parser's pos column and at the same time is the entry(hbase table) parser's entry's column
    public  String eventBytesCol = "eventBytes";

    //constructor and getter and setter
    public HBaseOperator() {
        conf = HBaseConfiguration.create();
        //import the xml configuration
        //conf.addResource("conf/hbase-site.xml");
        conf.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        conf.set("hbase.cluster.distributed","true");
        conf.set("hbase.zookeeper.quorum","localhost");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("dfs.socket.timeout", "180000");


    }

    public HBaseOperator(String myId) {
        conf = HBaseConfiguration.create();
        //import the xml configuration
        //conf.addResource("conf/hbase-site.xml");
        conf.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        conf.set("hbase.cluster.distributed","true");
        conf.set("hbase.zookeeper.quorum","localhost");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("dfs.socket.timeout", "180000");
        mysqlId = myId;
        trackerRowKey = trackerRowKey + "###" + mysqlId;
        parserRowKey = parserRowKey + "###" + mysqlId;
    }

    public void connect() throws Exception {
        hEventReader = new HTable(conf, eventBytesSchemaName);
        hEventWriter = new HTable(conf, eventBytesSchemaName);
        hEntryWriter = new HTable(conf, entryDataSchemaName);
        hEntryReader = new HTable(conf, entryDataSchemaName);
        hCheckpointWriter = new HTable(conf, checkpointSchemaName);
        hCheckpointReader = new HTable(conf, checkpointSchemaName);
    }

    public void disconnect() throws Exception {
        hEventReader.close();
        hEventWriter.close();
        hEntryReader.close();
        hEntryWriter.close();
        hCheckpointReader.close();
        hCheckpointWriter.close();
    }

    private HTable getHTableWriterBySchema(String schema) {
        if(schema.equals(eventBytesSchemaName)) return hEventWriter;
        if(schema.equals(entryDataSchemaName)) return hEntryWriter;
        if(schema.equals(checkpointSchemaName)) return hCheckpointWriter;
        return null;
    }

    private HTable getHTableReaderBySchema(String schema) {
        if(schema.equals(eventBytesSchemaName)) return hEventReader;
        if(schema.equals(entryDataSchemaName)) return hEntryReader;
        if(schema.equals(checkpointSchemaName)) return hCheckpointReader;
        return null;
    }

    public Configuration getConf() {
        return conf;
    }

    public String getEventBytesSchemaName() {
        return eventBytesSchemaName;
    }

    public String getCheckpointSchemaName() {
        return checkpointSchemaName;
    }

    public String getEntryDataSchemaName() {
        return entryDataSchemaName;
    }

    //single get data, single row multiple col,so multiple bytes
    public List<byte[]> getHBaseData(byte[] rowKey, String schemaName) throws IOException {
        HTable hTable = new HTable(conf, schemaName);
        Get get = new Get(rowKey);
        get.addFamily(getFamily(schemaName));
        Result result = hTable.get(get);
        List<byte[]> colValue = null;
        for(KeyValue kv:result.raw()){
            colValue .add(kv.getValue());
        }
        hTable.close();
        return(colValue);
    }

    //variable get data
    public Result getHBaseData(Get get, String schemaName) throws IOException {
        HTable hTable = getHTableReaderBySchema(schemaName);
        Result result = hTable.get(get);
        return(result);
    }

    public boolean existHBaseData(Get get, String schemaName) throws IOException {
        HTable hTable = new HTable(conf, schemaName);
        return hTable.exists(get);
    }

    public Result[] getHBaseData(List<Get> gets, String schemaName) throws IOException {
        HTable hTable = new HTable(conf, schemaName);
        Result[] results = hTable.get(gets);
        return results;
    }

    //variable scan data
    public ResultScanner getHBaseData(Scan scan, String schemaName) throws IOException {
        HTable hTable = getHTableReaderBySchema(schemaName);
        ResultScanner results = hTable.getScanner(scan);
        return(results);
    }

    //batched get data, multiple row so multiple row * multiple column,so multiple row * multiple col =
    //multiple * multiple bytes
    public Map<byte[],List<byte[]>> getHBaseData(byte[] startRowKey, byte[] endRowKey, String schemaName) throws IOException{
        HTable hTable = new HTable(conf, schemaName);
        Map<byte[],List<byte[]>> subTable = new HashMap<byte[], List<byte[]>>();
        Scan scan = new Scan();
        scan.addFamily(getFamily(schemaName));
        scan.setStartRow(startRowKey);
        //if endRowKey > hbase' all rowKey, exceed the boundary, will return null colValue????
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1maybe a potential bug
        scan.setStopRow(endRowKey);//do not include this, so endRowKey = startRowKey + insertRowNum
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1maybe a potential bug
        scan.setBatch(1000);
        ResultScanner results = hTable.getScanner(scan);
        for(Result result : results){
            byte[] elemRowKey = result.getRow();
            List<byte[]> elemColValue = new ArrayList<byte[]>();
            for(KeyValue kv : result.raw()){
                elemColValue.add(kv.getValue());
            }
            subTable.put(elemRowKey,elemColValue);
        }
        hTable.close();
        return(subTable);
    }

    //single or batched put data
    public void putHBaseData(List<byte[]> rowKeys, List<byte[]> colValues, String schemaName) throws IOException{
        HTable hTable = new HTable(conf, schemaName);
        List<Put> putList = new ArrayList<Put>();
        for(int i=0;i<=rowKeys.size()-1;i++){
            Put put = new Put(rowKeys.get(i));
            if(i>colValues.size()-1) put.add(getFamily(schemaName),null,null);
            else put.add(getFamily(schemaName),null,colValues.get(i));
            putList.add(put);
        }
        hTable.put(putList);
        hTable.close();
    }

    public void putHBaseData(List<byte[]> rowKeys, Map<byte[],List<byte[]>> subTable, String schemaName) throws IOException{
        HTable hTable = new HTable(conf, schemaName);
        List<Put> putList = new ArrayList<Put>();
        for(int i=0;i<=rowKeys.size()-1;i++){
            Put put = new Put(rowKeys.get(i));
            List<byte[]> columns = getColumn(schemaName,rowKeys.get(i).toString());
            int j = 0;
            for(byte[] bytes : subTable.get(rowKeys.get(i))){
                put.add(getFamily(schemaName),columns.get(j),bytes);
                j++;
            }
            putList.add(put);
        }
        hTable.put(putList);
        hTable.close();
    }

    public void putHBaseData(List<Put> puts, String schemaName) throws IOException{
        HTable hTable = getHTableWriterBySchema(schemaName);
        hTable.put(puts);
    }

    public void putHBaseData(Put put, String schemaName) throws  IOException{
        HTable hTable = getHTableWriterBySchema(schemaName);
        hTable.put(put);
    }

    public void deleteHBaseData(Delete del, String schemaName) throws IOException {
        HTable hTable = getHTableWriterBySchema(schemaName);
        hTable.delete(del);
    }

    public byte[] getFamily(String schemaName){
        return(Bytes.toBytes("d"));
    }
    public byte[] getFamily(){
        return(Bytes.toBytes("d"));
    }

    private List<byte[]> getColumn(String schemaName, String rowKey){
        String[] columns = null;
        List<byte[]> columnBytes = null;
        if(schemaName.equals(entryDataSchemaName)){
            columns = new String[]{"EventBytes"};
        }
        else if(schemaName.equals(checkpointSchemaName)){
            if(rowKey.contains("com/github/hackerwin7/mysql/tracker/tracker")) {
                columns = new String[]{"BinlogXid", "EventXidRowKey"};
            }
            else if(rowKey.contains("parser")){
                columns = new String[]{"EventRowKey","EntryRowKey"};
            }
            else{
                columns = null;
            }
        }
        for(String col : columns){
            columnBytes.add(Bytes.toBytes(col));
        }
        return(columnBytes);
    }

}
