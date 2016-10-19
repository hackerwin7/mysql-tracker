package com.github.hackerwin7.mysql.tracker.tracker;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogContext;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.tracker.tracker.parser.LogEventConvert;
import com.github.hackerwin7.mysql.tracker.tracker.utils.TrackerConfiger;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogDecoder;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.QueryLogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.server.ResultSetPacket;
import com.github.hackerwin7.mysql.tracker.hbase.driver.HBaseOperator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import com.github.hackerwin7.mysql.tracker.tracker.position.EntryPosition;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-9-16.
 */
public class MysqlTrackerHBase {

    //tracker's log
    private Logger logger = LoggerFactory.getLogger(MysqlTrackerHBase.class);

    //mysql JDBC by socket
    private MysqlConnector connector;

    private MysqlQueryExecutor queryExecutor ;

    private MysqlUpdateExecutor updateExecutor ;

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

    private int batchsize = 3000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second

    private double secondsize = 1.5;

    //per second write the position
    private int secondPer = 60;

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


    //constructor
    public MysqlTrackerHBase(TrackerConfiger configer) {
        this.configer = configer;
    }

    public MysqlTrackerHBase(String username, String password, String address, int port, Long slaveId) {
        configer = new TrackerConfiger(username, password, address, port, slaveId);
    }

    //getter and setter

    //prepare the configuration and connector and position for binlog dump
    private void preBinlogDump()throws IOException {
        //log comment
        logger.info("tracker is start successfully......");
        //initialize the connector and executor
        connector = new MysqlConnector(new InetSocketAddress(configer.getAddress(),configer.getPort()),
                configer.getUsername(),
                configer.getPassword());
        connectorTable = new MysqlConnector(new InetSocketAddress(configer.getAddress(),configer.getPort()),
                configer.getUsername(),
                configer.getPassword());
        //connect mysql to find start position and dump binlog
        try {
            connector.connect();
            connectorTable.connect();
        } catch (IOException e){
            logger.error("connector connect failed or connectorTable connect failed");
            throw new NullPointerException("connection failed!");
        }
        queryExecutor = new MysqlQueryExecutor(connector);
        updateExecutor = new MysqlUpdateExecutor(connector);
        //hbase operator
        hbaseOP = new HBaseOperator();
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
        eventQueue = new LinkedBlockingQueue<LogEvent>();
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

    //find position by mysql
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

    //dump the binlog data according to the start position
    private void binlogDump() throws IOException{
        //Thread : take the binlog data from the mysql
        logger.info("start the tracker thread to dump the binlog data from mysql...");
        FetchThread takeData = new FetchThread();
        takeData.start();
        //Thread :  per minute get the event
        logger.info("start the minute thread to save the position per minute as checkpoint...");
        PerminTimer minTask = new PerminTimer();
        Timer timer = new Timer();
        timer.schedule(minTask, 1000, secondPer * 1000);
        //persistence demand condition binlog data
        //#############################################################
        logger.info("start the persistence thread to persistent the entries and the last event position...");
        PersistenceThread persisData = new PersistenceThread();
        persisData.start();
        running = true;
        while(running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    private byte[] LongToStringToBytes(Long value){
        String strVal = String.valueOf(value);
        return(Bytes.toBytes(strVal));
    }

    private Long BytesToStringToLong(byte[] value){
        String strVal = new String(value);
        return(Long.valueOf(strVal));
    }




    //Thread : take the binlog data to List from tracker.common queue if List.size or time demand the condition
    //         and the last event is end event then we persistence the list all and clear start time and list

    class PersistenceThread extends Thread{

        private boolean running = true;

        private List<LogEvent> eventList;

        private Logger logger = LoggerFactory.getLogger(PersistenceThread.class);

        public void run(){
            startTime = new Date().getTime();
            eventList = new ArrayList<LogEvent>();
            logger.info("get the queue data to the local list");
            //first start the persistence Thread ,
            // we get the entryBytesPosition =  entryBytesXidPosition
            while(running){
                //while + sleep
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    logger.error("sleep error");
                    e.printStackTrace();
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
                    try {
                        logger.info("persistence the entry list data to the local disk");
                        //read the start pos by global and write the new start pos of event row key
                        // for tracker to global
                        writeHBaseEvent();
                    } catch (Exception e){
                        logger.error("persistence event list error");
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
            }
        }

        private void writeHBaseEvent() throws IOException{
            byte[] startPos = globalEventRowKey;
            List<Put> puts = new ArrayList<Put>();
            for(LogEvent event : eventList){
                CanalEntry.Entry entry = null;
                try {
                    entry = eventParser.parse(event);
                } catch (Exception e){
                    logger.error("parse to entry failed!!!");
                    e.printStackTrace();
                }
                //globalize
                globalBinlogName = eventParser.getBinlogFileName();
                if(entry!=null) {
                    Put put = new Put(startPos);
                    put.add(hbaseOP.getFamily(), Bytes.toBytes(hbaseOP.eventBytesCol), entry.toByteArray());
                    puts.add(put);
                    //get next pos
                    startPos = Bytes.toBytes(Bytes.toLong(startPos) + 1L);
                    //update to global xid,
                    // checkpoint pos record the xid event or row key ' s next pos not current pos
                    if(isEndEvent(event)){
                        //globalize
                        globalXidEvent = event;
                        globalXidEventRowKey = startPos;
                    }
                }
            }
            hbaseOP.putHBaseData(puts, hbaseOP.getEventBytesSchemaName());
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

    }

    //Thread :  per minute get the event
    class PerminTimer extends TimerTask {

        private Logger logger = LoggerFactory.getLogger(PerminTimer.class);

        @Override
        public void run(){
            logger.info("per minute persistence the binlog xid and event xid to checkpoint...");
            if(globalBinlogName != null && globalXidEvent != null && globalXidEventRowKey != null) {
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String time = sdf.format(cal.getTime());
                String rowKey = hbaseOP.trackerRowKey + ":" + time;
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
            }
        }
    }

    //Thread : take the binlog data from the mysql
    class FetchThread extends Thread{

        //mysql.dbsync interface
        private DirectLogFetcherChannel fetcher;

        private LogDecoder decoder;

        private LogContext context;

        private Logger logger = LoggerFactory.getLogger(FetchThread.class);

        public void run()  {
            try {
                preRun();
                while (fetcher.fetch()) {
                    logger.info("fetch the binlog data (event) successfully...");
                    LogEvent event = decoder.decode(fetcher, context);
                    if (event == null) {logger.error("fetched event is null!!!");throw new NullPointerException("event is null!");}
                    System.out.print("---------------->get event : " +
                                    LogEvent.getTypeName(event.getHeader().getType()) +
                                    ",----> now pos: " +
                                    (event.getLogPos() - event.getEventLen()) +
                                    ",----> next pos: " +
                                    event.getLogPos() +
                                    ",----> binlog file : " +
                                    eventParser.getBinlogFileName()
                    );
                    if(event.getHeader().getType() == LogEvent.QUERY_EVENT){
                        System.out.print(",----> sql : " +
                                        ((QueryLogEvent)event).getQuery()
                        );
                    }
                    System.out.println();
                    try {
                        if(event!=null) eventQueue.put(event);
                    } catch (InterruptedException e){
                        logger.error("eventQueue and entryQueue add data failed!!!");
                        throw new InterruptedIOException();
                    }
                }
            } catch (IOException e){
                logger.error("fetch data failed!!!");
                e.printStackTrace();
            }
            running = false;
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

    //main process
    public void mainProc()throws IOException{
        preBinlogDump();
        binlogDump();
        afterBinlogDump();
    }


    private void afterBinlogDump() throws IOException{
        connector.disconnect();
        connectorTable.disconnect();

    }
}
