package com.github.hackerwin7.mysql.tracker.tracker;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogContext;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.tracker.tracker.parser.LogEventConvert;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.bdp.magpie.MagpieExecutor;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.tracker.protocol.json.ConfigJson;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogDecoder;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.QueryLogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.server.ResultSetPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.tracker.filter.FilterMatcher;
import com.github.hackerwin7.mysql.tracker.hbase.driver.HBaseOperator;
import com.github.hackerwin7.mysql.tracker.monitor.TrackerMonitor;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import com.github.hackerwin7.mysql.tracker.tracker.position.EntryPosition;
import com.github.hackerwin7.mysql.tracker.tracker.utils.TrackerConfiger;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-9-22.
 */
public class HandlerForMagpieHBase implements MagpieExecutor {


    //tracker's log
    private Logger logger = LoggerFactory.getLogger(HandlerForMagpieHBase.class);

    //mysql JDBC by socket
    private MysqlConnector connector;

    private MysqlQueryExecutor queryExecutor ;

    private MysqlUpdateExecutor updateExecutor ;

    //get mysql position at real time
    private MysqlConnector realConnector;

    private MysqlQueryExecutor realQuery;

    //get the table structure
    private MysqlConnector connectorTable;

    //table meta cache
    private TableMetaCache tableMetaCache;

    //log event convert
    LogEventConvert eventParser;

    //configuration for tracker of mysql
    private TrackerConfiger configer ;

    //entry position, manager the offset, for file
    private EntryPosition startPosition ;

    //HBase Operator
    private HBaseOperator hbaseOP;

    // batch size threshold for per fetch the number of the event,if event size >= batchsize then
    // bigFetch() return
    // now by the py test we set the var is 1000

    private int batchsize = 100000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second

    private double secondsize = 1.0;

    //per second write the position
    private int secondPer = 60;

    //static queue max size
    private final int MAXQUEUE = 30000;

    //multi Thread share queue
    private BlockingQueue<LogEvent> eventQueue ;

    //Global variables
    private LogEvent globalEvent = null;

    private LogEvent globalXidEvent = null;

    private String globalBinlogName = null;

    private byte[] globalEventRowKey = null;

    private byte[] globalXidEventRowKey = null;

    private byte[] globalEntryRowKey = null;

    //control variable
    private boolean running;

    private long startTime;

    //run control
    private List<LogEvent> eventList;

    //monitor
    private TrackerMonitor monitor;

    //thread
    FetchThread takeData;
    PerminTimer minTask;
    Timer timer;

    //record job id
    private String jobId;

    //global thread communicate
    private int globalFetchThread = 0;

    //filter
    private FilterMatcher fm;



    //constructor
    public HandlerForMagpieHBase(TrackerConfiger configer) {
        this.configer = configer;
    }

    public HandlerForMagpieHBase(File file) throws IOException{
        if(file.exists()) {
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            Properties pro = new Properties();
            pro.load(in);
            configer = new TrackerConfiger();
            configer.setAddress(pro.getProperty("mysql.address"));
            configer.setPort(Integer.valueOf(pro.getProperty("mysql.port")));
            configer.setUsername(pro.getProperty("mysql.usr"));
            configer.setPassword(pro.getProperty("mysql.psd"));
            configer.setSlaveId(Long.valueOf(pro.getProperty("mysql.slaveId")));
            configer.setHbaseRootDir(pro.getProperty("hbase.rootdir"));
            configer.setHbaseDistributed(pro.getProperty("hbase.cluster.distributed"));
            configer.setHbaseZkQuorum(pro.getProperty("hbase.zookeeper.quorum"));
            configer.setHbaseZkPort(pro.getProperty("hbase.zookeeper.property.clientPort"));
            configer.setDfsSocketTimeout(pro.getProperty("dfs.socket.timeout"));
        } else {
            logger.error("properties file is not found !!! can not load the task!!!");
            System.exit(1);
        }
    }

    public void prepare(String id) throws Exception {
        //job id
        jobId = id;
        //adjust the config
        ConfigJson configJson = new ConfigJson(id);
        JSONObject jRoot = configJson.getJson();
        if(jRoot != null) {
            JSONObject jContent = jRoot.getJSONObject("info").getJSONObject("content");
            configer.setUsername(jContent.getString("Username"));
            configer.setPassword(jContent.getString("Password"));
            configer.setAddress(jContent.getString("Address"));
            configer.setPort(jContent.getInt("Port"));
            configer.setSlaveId(jContent.getLong("SlaveId"));
            configer.setHbaseRootDir(jContent.getString("HbaseRootDir"));
            configer.setHbaseDistributed(jContent.getString("HbaseDistributed"));
            configer.setHbaseZkQuorum(jContent.getString("HbaseZkQuorum"));
            configer.setHbaseZkPort(jContent.getString("HbaseZkPort"));
            configer.setDfsSocketTimeout(jContent.getString("DfsSocketTimeout"));
        }

        //log comment
        logger.info("starting the  tracker ......");
        //initialize the connector and executor
        boolean mysqlExist = true;
        do {
            connector = new MysqlConnector(new InetSocketAddress(configer.getAddress(), configer.getPort()),
                    configer.getUsername(),
                    configer.getPassword());
            connectorTable = new MysqlConnector(new InetSocketAddress(configer.getAddress(), configer.getPort()),
                    configer.getUsername(),
                    configer.getPassword());
            realConnector = new MysqlConnector(new InetSocketAddress(configer.getAddress(), configer.getPort()),
                    configer.getUsername(),
                    configer.getPassword());
            //connect mysql to find start position and dump binlog
            try {
                connector.connect();
                connectorTable.connect();
                realConnector.connect();
                mysqlExist = true;
            } catch (IOException e) {
                logger.error("connector connect failed or connectorTable connect failed");
                e.printStackTrace();
                logger.error("the mysql " + configer.getAddress() + " is not available ...");
                mysqlExist = false;
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ei) {
                    ei.printStackTrace();
                }
            }
        } while(!mysqlExist);
        queryExecutor = new MysqlQueryExecutor(connector);
        updateExecutor = new MysqlUpdateExecutor(connector);
        realQuery = new MysqlQueryExecutor(realConnector);
        //hbase operator
        hbaseOP = new HBaseOperator(id);
        hbaseOP.getConf().set("hbase.rootdir",configer.getHbaseRootDir());
        hbaseOP.getConf().set("hbase.cluster.distributed",configer.getHbaseDistributed());
        hbaseOP.getConf().set("hbase.zookeeper.quorum",configer.getHbaseZkQuorum());
        hbaseOP.getConf().set("hbase.zookeeper.property.clientPort",configer.getHbaseZkPort());
        hbaseOP.getConf().set("dfs.socket.timeout", configer.getDfsSocketTimeout());
        hbaseOP.connect();
        //find start position
        //log comment
        logger.info("find start position");
        startPosition = findStartPosition();
        if(startPosition == null) throw new NullPointerException("start position is null");
        //get the table structure
        tableMetaCache = new TableMetaCache(connectorTable);
        //initialize the log event convert (to the entry)
        eventParser = new LogEventConvert();
        eventParser.setTableMetaCache(tableMetaCache);
        //queue
        eventQueue = new LinkedBlockingQueue<LogEvent>(MAXQUEUE);

        //thread start
        //Thread : take the binlog data from the mysql
        logger.info("start the tracker thread to dump the binlog data from mysql...");
        globalFetchThread = 0;
        takeData = new FetchThread();
        takeData.start();
        //Thread :  per minute get the event
        logger.info("start the minute thread to save the position per minute as checkpoint...");
        minTask = new PerminTimer();
        timer = new Timer();
        timer.schedule(minTask, 1000, secondPer * 1000);

        //run() control
        startTime = new Date().getTime();
        eventList = new ArrayList<LogEvent>();

        //monitor
        monitor = new TrackerMonitor();

        //log
        logger.info("tracker is started successfully......");

        //filter
        fm = new FilterMatcher(configer.getFilterRegex());
    }


    //find start position include binlog file name and offset
    private EntryPosition findStartPosition()throws IOException{
        EntryPosition entryPosition;
        //load form file
        entryPosition = findHBaseStartPosition();
        if(entryPosition == null){
            //load from mysql
            logger.info("file position load failed , get the position from mysql!");
            entryPosition = findMysqlStartPosition();
        }
        else{
            logger.info("file position loaded!");
        }
        return(entryPosition);
    }

    //find position from HBase
    private EntryPosition findHBaseStartPosition() throws IOException{
        EntryPosition entryPosition = null;
        Get get = new Get(Bytes.toBytes(hbaseOP.trackerRowKey));
        get.addFamily(hbaseOP.getFamily());
        Result result = hbaseOP.getHBaseData(get,hbaseOP.getCheckpointSchemaName());
        for(KeyValue kv : result.raw()){
            byte[] value = kv.getValue();
            if(value != null) {
                String binXid = new String(value);
                if (binXid.contains(":")) {
                    String[] dataSplit = binXid.split(":");
                    entryPosition = new EntryPosition(dataSplit[0], Long.valueOf(dataSplit[1]));
                } else {
                    String stringValue = Bytes.toString(value);
                    Long longValue = Long.valueOf(stringValue);
                    globalEventRowKey = Bytes.toBytes(longValue);
                }
            }
        }
        return(entryPosition);
    }

    //find position by mysql  note!!! : this function will change the variable globalEventRowKey
    private EntryPosition findMysqlStartPosition()throws IOException{
        ResultSetPacket resultSetPacket = queryExecutor.query("show master status");
        List<String> fields = resultSetPacket.getFieldValues();
        if(CollectionUtils.isEmpty(fields)){
            throw new NullPointerException("show master status failed!");
        }
        //binlogXid
        EntryPosition entryPosition = new EntryPosition(fields.get(0),Long.valueOf(fields.get(1)));
        //eventXid
        Long pos = 0L;
        globalEventRowKey = Bytes.toBytes(pos);
        return(entryPosition);
    }


    class PerminTimer extends TimerTask {

        private Logger logger = LoggerFactory.getLogger(PerminTimer.class);

        @Override
        public void run(){
            //monitor the mysql connection , is connection is invalid then close all thread
            if(globalBinlogName != null && globalXidEvent != null && globalXidEventRowKey != null) {
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String time = sdf.format(cal.getTime());
                String rowKey = hbaseOP.trackerRowKey + "##" + time;
                Put put = new Put(Bytes.toBytes(rowKey));
                String xidValue = globalBinlogName + ":" + globalXidEvent.getLogPos();
                Long xidEventRowLong = Bytes.toLong(globalXidEventRowKey);
                String xidEventRowString = String.valueOf(xidEventRowLong);
                put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.binlogXidCol), Bytes.toBytes(xidValue));
                put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventXidCol), Bytes.toBytes(xidEventRowString));
                try {
                    hbaseOP.putHBaseData(put, hbaseOP.getCheckpointSchemaName());
                } catch (IOException e) {
                    logger.error("per minute persistence failed!!!");
                    e.printStackTrace();
                }
                logger.info("======> per minute persistence the binlog xid and event xid to checkpoint : ");
                logger.info("---> row kye is " + rowKey +
                        ",col is :" + xidValue + "; col is :" + xidEventRowString);
            }
        }
    }

    //Thread : take the binlog data from the mysql
    class FetchThread extends Thread {

        //mysql.dbsync interface
        private DirectLogFetcherChannel fetcher;

        private LogDecoder decoder;

        private LogContext context;

        private Logger logger = LoggerFactory.getLogger(FetchThread.class);

        private LogEvent event;

        private TrackerMonitor monitor = new TrackerMonitor();

        public void run() {
            try {
                preRun();
                int counter = 0;
                while (fetcher.fetch()) {
                    if(counter == 0) monitor.fetchStart = System.currentTimeMillis();
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        logger.warn("fetched event is null!!!");
                        continue;
                    }
                    counter++;
                    monitor.batchSize += event.getEventLen();
                    //add the event to the queue
                    try {
                        if (event != null) eventQueue.put(event);
                    } catch (InterruptedException e) {
                        logger.error("eventQueue and entryQueue add data failed!!!");
                        throw new InterruptedIOException();
                    }
                    if(counter % 10000 == 0) {
                        monitor.fetchEnd = System.currentTimeMillis();
                        logger.info("======> fetch thread : ");
                        logger.info("---> fetch during time : " + (monitor.fetchEnd - monitor.fetchStart));
                        logger.info("---> fetch number : " + counter);
                        logger.info("---> fetch sum size : " + monitor.batchSize);
                        monitor.clear();
                        counter = 0;
                    }
                }
            } catch (IOException e) {
                logger.error("fetch data failed!!! the IOException is " + e.getMessage());
                e.printStackTrace();
                String errMsg = e.getMessage();
                if(errMsg.contains("errno = 1236")) {// the position or logfile error, reset the position to show master status
                    Delete del = new Delete(Bytes.toBytes(hbaseOP.trackerRowKey));
                    try {
                        hbaseOP.deleteHBaseData(del,hbaseOP.getCheckpointSchemaName());
                        close(jobId);
                    } catch (Exception e1) {
                        logger.error("delete the checkpoint row key failed ...... msg : " + e1.getMessage());
                    }
                    globalFetchThread = 1;
                }
            }
        }

        public void preRun() throws IOException {
            //binlog dump thread configuration
            logger.info("set the binlog configuration for the binlog dump");
            updateExecutor.update("set wait_timeout=9999999");
            updateExecutor.update("set net_write_timeout=1800");
            updateExecutor.update("set net_read_timeout=1800");
            updateExecutor.update("set names 'binary'");//this will be my try to test no binary
            updateExecutor.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
            updateExecutor.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
            //send binlog dump packet and mysql will establish a binlog dump thread
            logger.info("send the binlog dump packet to mysql , let mysql set up a binlog dump thread in mysql");
            BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
            binDmpPacket.binlogFileName = startPosition.getJournalName();
            binDmpPacket.binlogPosition = startPosition.getPosition();
            binDmpPacket.slaveServerId = configer.getSlaveId();
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(connector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            //initialize the mysql.dbsync to fetch the binlog data
            logger.info("initialize the mysql.dbsync class");
            fetcher = new DirectLogFetcherChannel(connector.getReceiveBufferSize());
            fetcher.start(connector.getChannel());
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }

    }


    public void reload(String id) {

    }


    public void pause(String id) throws Exception {

    }


    public void close(String id) throws Exception {
        minTask.cancel();
        timer.cancel();
        connector.disconnect();//stop the fetch thread the fetch data must stop
        connectorTable.disconnect();
        realConnector.disconnect();
        hbaseOP.disconnect();
    }


    public void run() throws Exception {
        //check the fetch thread's  status
        if(globalFetchThread == 1) {
            globalFetchThread = 0;
            throw new Exception("restart the magpie executor !!! ");
        }
        //take the data from the queue
        while(!eventQueue.isEmpty()) {
            try {
                LogEvent event = eventQueue.take();
                if(event!=null) eventList.add(event);
                //per turn do not load much data
                if(eventList.size() >= batchsize) break;
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        //persistence the batch size event data to event table
        if ((eventList.size() >= batchsize || new Date().getTime() - startTime > secondsize * 1000) ) {
            monitor.persisNum = eventList.size();
            try {
                //logger.info("persistence the entry list data to the local disk");
                //read the start pos by global and write the new start pos of event row key
                // for tracker to global
                writeHBaseEvent();
            } catch (Exception e){
                logger.error("persistence event list error msg : " + e.getMessage());
                e.printStackTrace();
            }
            if(existXid(eventList)){
                //persistence xid pos of binlog and event row key to checkpoint and
                //update global for per minute
                try {
                    writeHBaseCheckpointXid();
                } catch (IOException e) {
                    logger.error("persistence xid pos error");
                    e.printStackTrace();
                }
            }
            //after persistence reinitialize the state
            eventList.clear();//the position is here???
            startTime = new Date().getTime();
        }
        monitor.persistenceEnd = System.currentTimeMillis();
        if(monitor.persisNum > 0) {
            logger.info("---> persistence deal during time : " + (monitor.persistenceEnd - monitor.persistenceStart));
            logger.info("---> write hbase during time : " + (monitor.hbaseWriteEnd - monitor.hbaseWriteStart));
            logger.info("---> entry list to bytes sum size is " + monitor.batchSize);
            logger.info("---> the number if entry list is " + monitor.persisNum);
            monitor.clear();
        }
    }

    private void writeHBaseEvent() throws IOException {
        byte[] startPos = globalEventRowKey;
        List<Put> puts = new ArrayList<Put>();
        LogEvent lastEvent = null;
        CanalEntry.Entry lastRowEntry = null;
        CanalEntry.Entry entry = null;
        monitor.persistenceStart = System.currentTimeMillis();
        for(LogEvent event : eventList){
            lastEvent = event;
            try {
                entry = eventParser.parse(event);
                //filter !!!! it should not continue in here because we lose it may be xid to persistence position for mysql,so the  globalXidEvent and globalXidEventRowKey may be incorrect
                if(!fm.isMatch(entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName())) continue;
            } catch (Exception e){
                logger.error("parse to entry failed!!!");
                e.printStackTrace();
            }
            if(entry != null && entry.getEntryType() == CanalEntry.EntryType.ROWDATA) lastRowEntry = entry;
            //log monitor
            //logInfoEvent(event);
            //globalize
            globalBinlogName = eventParser.getBinlogFileName();
            if(entry!=null) {
                Put put = new Put(startPos);
                byte[] entryBytes = entry.toByteArray();
                monitor.batchSize += entryBytes.length;
                put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventBytesCol), entryBytes);
                puts.add(put);
                //get next pos
                startPos = Bytes.toBytes(Bytes.toLong(startPos) + 1L);
                //u pdate to global xid,
                // checkpoint pos record the xid event or row key ' s next pos not current pos
                if(isEndEvent(event)){
                    //globalize
                    globalXidEvent = event;
                    globalXidEventRowKey = startPos;
                }
            }
        }
        monitor.persistenceEnd = System.currentTimeMillis();
        if(lastEvent != null && lastRowEntry != null) {
            if(eventList.size() > 0)
                logger.info("======> persistence the " + eventList.size() + " events "
                + " the batched last column is " + getEntryCol(lastRowEntry));
            logInfoEvent(lastEvent);
            logInfoBatchEvent(lastEvent);
        }
        monitor.hbaseWriteStart = System.currentTimeMillis();
        hbaseOP.putHBaseData(puts, hbaseOP.getEventBytesSchemaName());
        monitor.hbaseWriteEnd = System.currentTimeMillis();
        //globalize, checkpoint pos record the xid event or row key ' s next pos not current pos
        globalEventRowKey = startPos;
    }

    private boolean existXid(List<LogEvent> eventList){
        for(LogEvent event : eventList){
            if(isEndEvent(event)){
                return(true);
            }
        }
        return(false);
    }

    private boolean isEndEvent(LogEvent event){
        if((event.getHeader().getType()==LogEvent.XID_EVENT)
                ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN"))){
            return (true);
        }
        else    return(false);
    }

    private void writeHBaseCheckpointXid() throws IOException{
        Put put = new Put(Bytes.toBytes(hbaseOP.trackerRowKey));
        String xidValue = globalBinlogName + ":" + globalXidEvent.getLogPos();
        Long xidEventRowLong = Bytes.toLong(globalXidEventRowKey);
        String xidEventRowString = String.valueOf(xidEventRowLong);
        put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.binlogXidCol), Bytes.toBytes(xidValue));
        put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventXidCol), Bytes.toBytes(xidEventRowString));
        hbaseOP.putHBaseData(put, hbaseOP.getCheckpointSchemaName());
    }

    private long getDelay(LogEvent event) {
        return new Date().getTime() - event.getWhen();
    }

    private void logInfoBatchEvent(LogEvent lastEvent) {
        //monitor measurement for delay time
        if(lastEvent != null) {
            try {
                //monitor measurement for over stock
                ResultSetPacket resultSetPacket = realQuery.query("show master status");
                List<String> fields = resultSetPacket.getFieldValues();
                if(CollectionUtils.isEmpty(fields)){
                    throw new NullPointerException("show master status failed!");
                }
                //binlogXid
                EntryPosition nowPos = new EntryPosition(fields.get(0),Long.valueOf(fields.get(1)));
                long overStock = nowPos.getPosition() - lastEvent.getLogPos();
                logger.info("---> batch over stock : " + overStock);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void logInfoEvent(LogEvent lastEvent) {
        //monitor measurement for delay time
        if(lastEvent != null) {
            try {
                long deleyTime = getDelay(lastEvent);
                logger.info("--->get event : " +
                                LogEvent.getTypeName(lastEvent.getHeader().getType()) +
                                ", now pos: " +
                                (lastEvent.getLogPos() - lastEvent.getEventLen()) +
                                ", next pos: " +
                                lastEvent.getLogPos() +
                                ", binlog file : " +
                                eventParser.getBinlogFileName() +
                                ", delay time : " +
                                deleyTime +
                                ", type : " +
                                getEventType(lastEvent)
                );
                if (lastEvent.getHeader().getType() == LogEvent.QUERY_EVENT) {
                    logger.info(", sql : " +
                                    ((QueryLogEvent) lastEvent).getQuery()
                    );
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String getEntryCol(CanalEntry.Entry entry) {
        String colValue = "";
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            if (rowChange.getRowDatasList().size() > 0) {
                CanalEntry.RowData rowData = rowChange.getRowDatas(0);
                if (rowData.getAfterColumnsList().size() > 0) {
                    colValue = rowData.getAfterColumns(0).getName() + " ## " + rowData.getAfterColumns(0).getValue();
                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return colValue;
    }

    private String getEventType(LogEvent event) {
        return event.getTypeName(event.getHeader().getType());
    }

    private void delaySec(int t) {
        try {
            Thread.sleep(t * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
