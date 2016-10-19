package com.github.hackerwin7.mysql.tracker.tracker;


import com.github.hackerwin7.mysql.tracker.mysql.dbsync.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogContext;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogDecoder;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.QueryLogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.server.ResultSetPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.PacketManager;
import org.apache.commons.lang.StringUtils;
//import org.jruby.RubyProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import com.github.hackerwin7.mysql.tracker.tracker.parser.LogEventConvert;
import com.github.hackerwin7.mysql.tracker.tracker.position.EntryPosition;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.tracker.tracker.utils.TrackerConfiger;


import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by hp on 14-9-2.
 */
public class MysqlTracker {


    //tracker's logger
    private Logger logger = LoggerFactory.getLogger(MysqlTracker.class);
    //private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MysqlTracker.class);

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

    //entry position, manager the offset
    private EntryPosition startPosition ;

    // batch size threshold for per fetch the number of the event,if event size >= batchsize then
    // bigFetch() return
    // now by the py test we set the var is 1000

    private int batchsize = 3000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second

    private double secondsize = 1.5;

    //event file data save
    private String eventDat = "event.dat";

    //entry file data save
    private String entryDat = "entry.dat";

    //event for thread
    private LogEvent eventThread = null;

    //position per minute save
    private String minuteDat = "minute.dat";

    //minute log File
    private String minLogFile ;

    //per second write the position
    private int secondPer = 60;

    //multi Thread share queue
    private BlockingQueue<LogEvent> eventQueue = new LinkedBlockingQueue<LogEvent>();



    public MysqlTracker(String username, String password, String address, int port, Long slaveId) {
        configer = new TrackerConfiger(username, password, address, port, slaveId);
    }

    public MysqlTracker(TrackerConfiger configer) {
        this.configer = configer;
    }


    //prepare the configuration and connector and position for binlog dump
    private void preBinlogDump()throws IOException{
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
    }

    //find start position include binlog file name and offset
    private EntryPosition findStartPosition()throws IOException{
        EntryPosition entryPosition;
        //load form file
        entryPosition = findFileStartPosition();
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

    //find position by file
    private EntryPosition findFileStartPosition() throws IOException{
        EntryPosition entryPosition = new EntryPosition();
        if(entryPosition.readBinlogPosFile()){
            if(!entryPosition.getJournalName().equals("")&&
                    entryPosition.getPosition()!=0){
                return(entryPosition);
            }
            else return(null);
        }
        else return(null);
    }

    //find position by mysql
    private EntryPosition findMysqlStartPosition()throws IOException{
        ResultSetPacket resultSetPacket = queryExecutor.query("show master status");
        List<String> fields = resultSetPacket.getFieldValues();
        if(CollectionUtils.isEmpty(fields)){
            throw new NullPointerException("show master status failed!");
        }
        EntryPosition entryPosition = new EntryPosition(fields.get(0),Long.valueOf(fields.get(1)));
        return(entryPosition);
    }


    //dump the binlog data according to the start position
    private void binlogDump() throws IOException{
        //Thread : take the binlog data from the mysql
        logger.info("start the tracker thread to dump the binlog data from mysql...");
        TakeDataThread takeData = new TakeDataThread();
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
        boolean running = true;
        while(running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    //Thread : take the binlog data to List from tracker.common queue if List.size or time demand the condition
    //         and the last event is end event then we persistence the list all and clear start time and list

    class PersistenceThread extends Thread{

        private List<CanalEntry.Entry> entryList;

        private CanalEntry.Entry entry;

        private long startTime;

        private boolean running = true;

        private LogEvent lastEvent;

        private List<LogEvent> eventList;

        private LogEvent event;

        private Logger logger = LoggerFactory.getLogger(PersistenceThread.class);

        public void run(){
            //bug! bug! bug!
            startTime = new Date().getTime();
            entryList = new ArrayList<CanalEntry.Entry>();
            eventList = new ArrayList<LogEvent>();
            lastEvent = null;
            logger.info("get the queue data to the local list");
            //first start the persistence Thread ,
            // we get the entryBytesPosition =  entryBytesXidPosition
            while(running){
                //take the data from the queue
                //it's really bug here
                //bug: if lastEvent!=null but it do not enter the if structure so while(running) ,
                //the lastEvent is null , the not null value is wrongly covered
                while(!eventQueue.isEmpty()) {
                    try {
                        event = eventQueue.take();
                        if(event!=null) eventList.add(event);
                        try {
                            entry = eventParser.parse(event);
                        } catch (Exception e){
                            logger.error("event parse to entry failed!!!");
                            e.printStackTrace();
                        }
                        if(entry!=null) entryList.add(entry);
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
                //write entries to the disk
                //on the big transaction that may exceed the memory ,
                // the writeFileEntry may not
                //we set the entryList.size() is not suitable , because a single event(or entry) may include 10000000 rows data
                //probably, the (transaction begin ---> row event ----> transaction end).size = connector.receiveBufferedSize,
                //so we can judge the buffer size threshold to decide our persistence policy
                //bach processing the entries and xid event
                if ((entryList.size() >= batchsize || new Date().getTime() - startTime > secondsize * 1000) ) {
                    try {
                        logger.info("persistence the entry list data to the local disk");
                        writeFileEntry(entryList);
                    } catch (IOException e){
                        logger.error("persistence entry list error");
                        e.printStackTrace();
                    }
                    //write the entryBytesPosition,line number temporarily
                    try {
                        writeFileEntryBytesPosition((long)entryList.size());
                    } catch (IOException e) {
                        logger.error("write entry bytes position failed!!!");
                        e.printStackTrace();
                    }
                    //write binlog position to disk (must be xid or end) in the entryList 's event if exists.
                    //and Do we need set the batch mode for position write???
                    //yes we need , we share the batch size with the entries
                    try {
                        writeBinlogPosition();
                    } catch (IOException e) {
                        logger.error("wirte binlog position is error");
                        e.printStackTrace();
                    }
                    //after persistence initialize the state
                    entryList.clear();
                    eventList.clear();//the position is here???
                    startTime = new Date().getTime();
                }
            }
        }

        private void writeFileEntryBytesPosition(long increasedLines) throws IOException{
            startPosition.readEntryBytesWritePosFile();
            startPosition.setEntryBytesWriteLine(startPosition.getEntryBytesWriteLine() + increasedLines);
            startPosition.writeEntryBytesWritePosFile();
        }

        private void writeFileEntry(List<CanalEntry.Entry> entries) throws IOException{
            startPosition.setEntryList(entries);
            startPosition.writeEntryBytesCodeFile();
        }

        private void writeBinlogPosition() throws IOException{
            //find the eventList rear whether have xid
            lastEvent = null;
            for(LogEvent eventElem : eventList){
                if(isEndEvent(eventElem)){
                    lastEvent = eventElem;
                }
            }
            if(lastEvent != null) {
                logger.info("write Binlog Position process!");
                //write the xid position of binlog to File
                LogEvent event = lastEvent;
                //multiple thread the xid event maybe not in entryList 's event
                startPosition.setJournalName(eventParser.getBinlogFileName());
                startPosition.setPosition(event.getLogPos());
                startPosition.writeBinlogPosFile();
                //write the runtime for the per entry list
                long runTime = new Date().getTime() - startTime;
                BufferedWriter bw = new BufferedWriter(new FileWriter(startPosition.getBinlogPosFileName(), true));
                String dataRunTime = String.valueOf(runTime / 1000) + " s";
                bw.append(dataRunTime);
                bw.newLine();
                bw.flush();
                bw.close();
                //wirte the xid position of entry Bytes table to File,this position we set the line number

                //after process
                lastEvent = null;
            }
        }

        private boolean isEndEvent(LogEvent event){
            if((event.getHeader().getType()==LogEvent.XID_EVENT)
                    ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                    && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN"))){
                return (true);
            }
            else    return(false);
        }


    }

    //Thread : take the binlog data from the mysql
    class TakeDataThread extends Thread{


        private LogEvent event;

        //mysql.dbsync interface
        private DirectLogFetcherChannel fetcher;

        private LogDecoder decoder;

        private LogContext context;

        private Logger logger = LoggerFactory.getLogger(TakeDataThread.class);

        public void run()  {
            try {
                preRun();
                while (fetcher.fetch()) {
                    logger.info("fetch the binlog data (event) successfully...");
                    event = decoder.decode(fetcher, context);
                    if (event == null) {logger.error("fetched event is null!!!");throw new NullPointerException("event is null!");}
                    System.out.println("---------------->get event : " +
                            LogEvent.getTypeName(event.getHeader().getType()) +
                            ",----> now pos: " +
                            (event.getLogPos() - event.getEventLen()) +
                            ",----> next pos: " +
                            event.getLogPos() +
                            ",----> binlog file : " +
                            eventParser.getBinlogFileName());
                    if(isEndEvent(event)) eventThread = event;//for per minute event position record
                    minLogFile = eventParser.getBinlogFileName();
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

        private boolean isEndEvent(LogEvent event){
            if((event.getHeader().getType()==LogEvent.XID_EVENT)
                    ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                    && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN"))){
                return (true);
            }
            else    return(false);
        }

    }


    //Thread :  per minute get the event
    class PerminTimer extends TimerTask{

        private Logger logger = LoggerFactory.getLogger(PerminTimer.class);

        @Override
        public void run(){
            if(eventThread != null && minLogFile != null){
                EntryPosition minPosition = new EntryPosition();
                minPosition.setJournalName(minLogFile);
                minPosition.setPosition(eventThread.getLogPos());
                try {
                    minPosition.writeMinuteBinlogPosFile();
                }catch (IOException e){
                    logger.error("minute position write failed!");
                    throw new NullPointerException("min position write is failed!");
                }
            }
        }
    }





    public static String getStringFromByteArray(byte[] bytes){
        String inString = new String("");
        inString += String.valueOf(bytes[0]);
        for(int i=1;i<=bytes.length-1;i++){
            inString+=","+String.valueOf(bytes[i]);
        }
        return(inString);
    }

    public static byte[] getByteArrayFromString(String inString){
        String[] stringBytes = inString.split(",");
        byte[] bytes = new byte[stringBytes.length];
        for(int i=0;i<=stringBytes.length-1;i++){
            bytes[i] = Integer.valueOf(stringBytes[i]).byteValue();
        }
        return(bytes);
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


    //build the disMainPorc() function to solve the tracker rebuild process if the tracker encounter a crash


}


