package com.github.hackerwin7.mysql.tracker.tracker;

import com.github.hackerwin7.mysql.tracker.filter.FilterMatcher;
import com.github.hackerwin7.mysql.tracker.kafka.driver.producer.KafkaSender;
import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.tracker.monitor.JrdwMonitorVo;
import com.github.hackerwin7.mysql.tracker.monitor.TrackerMonitor;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogContext;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogDecoder;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.QueryLogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.server.ResultSetPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.tracker.protocol.json.JSONConvert;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.tracker.tracker.parser.LogEventConvert;
import com.github.hackerwin7.mysql.tracker.tracker.position.EntryPosition;
import com.github.hackerwin7.mysql.tracker.tracker.utils.TrackerConf;
import com.github.hackerwin7.mysql.tracker.zk.client.ZkExecutor;
import com.github.hackerwin7.mysql.tracker.zk.utils.ZkConf;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.bdp.magpie.MagpieExecutor;
import com.jd.bdp.monitors.commons.util.CheckpointUtil;
import com.jd.bdp.monitors.constants.JDMysqlTrackerMonitorType;
import kafka.producer.KeyedMessage;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-12-12.
 */
public class HandlerMagpieKafkaCheckpointZk implements MagpieExecutor {
    //logger
    private Logger logger = LoggerFactory.getLogger(HandlerMagpieKafkaCheckpointZk.class);
    //constants
    public static final String LOG_HEAD_LINE = "===================================================> ";
    //global config
    private TrackerConf config = new TrackerConf();
    //mysql interface
    private MysqlConnector logConnector;
    private MysqlConnector tableConnector;
    private MysqlConnector realConnector;
    private MysqlQueryExecutor queryExecutor;
    private MysqlUpdateExecutor updateExecutor;
    private MysqlQueryExecutor realQuery;
    //mysql table meta cache
    private TableMetaCache tableMetaCache;
    //mysql log event convert and filter
    private LogEventConvert eventConvert;
    //job id
    private String jobId;
    //kafka
    private KafkaSender msgSender;
    //phoenix kafka
    private KafkaSender phMonitorSender;
    //zk
    private ZkExecutor zkExecutor;
    //blocking queue
    private BlockingQueue<CanalEntry.Entry> entryQueue;
    //batch id and in batch id
    private long batchId = 0;
    private long inBatchId = 0;
    private String logfile;
    private long offset;
    //thread communicate
    private int globalFetchThread = 0;
    //global var
    private LogEvent globalXidEvent = null;
    private CanalEntry.Entry globalXidEntry = null;
    private String globalBinlogName = "null";
    private long globalXidBatchId = -1;
    private long globalXidInBatchId = -1;
    //filter
    private FilterMatcher fm;
    //global start time
    private long startTime;
    //thread and timer task job
    private Fetcher fetcher;
    private Timer cptimer;
    private Timer htimer;
    private HeartBeat heartBeat;
    private ConfirmCP confirm;
    //heartbeat alive
    private boolean isHeartBeatAlive = true;
    //monitor
    private TrackerMonitor monitor;
    //global var
    private List<CanalEntry.Entry> entryList;//filtered
    private LogEvent lastEvent = null;//get the eventList's last xid event
    private CanalEntry.Entry lastEntry = null;
    private String binlog = null;
    private List<KeyedMessage<String, byte[]>> messageList;
    //thread survival confirmation
    private boolean fetchSurvival = true;
    //thread is finish or run once
    private boolean isFetchRunning = false;
    //check position
    private CheckpointUtil cpUtil = null;
    //every minute save to the atomic cp
    private String ConCP = null;
    //debug var

    //delay time
    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private  void delayMin(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //init global config
    private void init() throws Exception {
        //log
        logger.info("initializing......");
        //id
        config.jobId = jobId;
        //init envrionment config (local,off-line,on-line)
        config.initConfOnlineJSON();//config.initConfJSON();//config.initConfStatic();
        //show the laod conf
        logger.info(config.toString());
        //jobId
        jobId = config.jobId;
        //load position
        logfile = config.logfile;
        offset = config.offset;
        batchId = config.batchId;
        inBatchId = config.inId;
        //phoenix monitor kafka
        KafkaConf kpcnf = new KafkaConf();
        kpcnf.brokerList = config.phKaBrokerList;
        kpcnf.port = config.phKaPort;
        kpcnf.topic = config.phKaTopic;
        kpcnf.acks = config.phKaAcks;
        phMonitorSender = new KafkaSender(kpcnf);
        phMonitorSender.connect();
        //generate the driver, interface etc.
        logConnector = new MysqlConnector(new InetSocketAddress(config.address, config.myPort),
                config.username,
                config.password);
        tableConnector = new MysqlConnector(new InetSocketAddress(config.address, config.myPort),
                config.username,
                config.password);
        realConnector = new MysqlConnector(new InetSocketAddress(config.address, config.myPort),
                config.username,
                config.password);
        boolean mysqlExists = false;
        int retryMysql = 0;
        while (!mysqlExists) {
            if(retryMysql >= config.retrys) {//reload
                globalFetchThread = 1;
                throw new RetryTimesOutException("reload job......");
            }
            retryMysql++;
            try {
                logConnector.connect();
                tableConnector.connect();
                realConnector.connect();
                mysqlExists = true;
            } catch (IOException e) {
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TrackerMonitor exMonitor = new TrackerMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                sendMonitor.start();
                logger.error("connect mysql failed ... retry to connect it...");
                e.printStackTrace();
                delay(5);
            }
        }
        queryExecutor = new MysqlQueryExecutor(logConnector);
        updateExecutor = new MysqlUpdateExecutor(logConnector);
        realQuery = new MysqlQueryExecutor(realConnector);
        //table meta cache
        tableMetaCache = new TableMetaCache(tableConnector);
        //queue
        entryQueue = new LinkedBlockingQueue<CanalEntry.Entry>(config.queuesize);
        //kafka
        KafkaConf kcnf = new KafkaConf();
        kcnf.brokerList = config.brokerList;
        kcnf.port = config.kafkaPort;
        kcnf.topic = config.topic;
        kcnf.acks = config.acks;
        kcnf.compression = TrackerConf.kafkaCompression;
        msgSender = new KafkaSender(kcnf);
        msgSender.connect();
        //zk
        ZkConf zcnf = new ZkConf();
        zcnf.zkServers = config.zkServers;
        zkExecutor = new ZkExecutor(zcnf);
        boolean isZk = false;
        int retryZk = 0;
        while (!isZk) {
            if(retryZk >= config.retrys) {
                globalFetchThread = 1;
                throw new RetryTimesOutException("reload job......");//reload
            }
            retryZk++;
            try {
                zkExecutor.connect();
                isZk = true;
            } catch (Exception e) {
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TrackerMonitor exMonitor = new TrackerMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                sendMonitor.start();
                logger.error("connect mysql failed ... retry to connect it...  exception is " + e.getMessage(), e);
                e.printStackTrace();
                delay(3);
            }
        }
        initZk();
        //filter
        fm = new FilterMatcher(config.filterRegex);
        //event convert
        eventConvert = new LogEventConvert();
        eventConvert.setTableMetaCache(tableMetaCache);
        eventConvert.setCharset(config.charset);
        eventConvert.filterMap.putAll(config.filterMap);
        //start time configuration
        startTime = System.currentTimeMillis();
        //global fetch thread
        globalFetchThread = 0;
        //thread config
        fetcher = new Fetcher();
        cptimer = new Timer();
        htimer = new Timer();
        heartBeat = new HeartBeat();
        confirm = new ConfirmCP();
        //monitor
        monitor = new TrackerMonitor();
        //global var
        entryList = new ArrayList<CanalEntry.Entry>();
        messageList = new ArrayList<KeyedMessage<String, byte[]>>();
        isFetchRunning = false;
        lastEntry = null;
        //checkpoint util
        cpUtil = new CheckpointUtil();
    }

    private void initZk() throws Exception {
        boolean isZk =false;
        int retryZk = 0;
        while (!isZk) {
            if(retryZk >= config.retrys) {
                globalFetchThread = 1;
                throw new RetryTimesOutException("reload job......");
            }
            retryZk++;
            try {
                if (!zkExecutor.exists(config.rootPath)) {
                    zkExecutor.create(config.rootPath, "");
                }
                if (!zkExecutor.exists(config.persisPath)) {
                    zkExecutor.create(config.persisPath, "");
                }
                if (!zkExecutor.exists(config.minutePath)) {
                    zkExecutor.create(config.minutePath, "");
                }
                isZk = true;
            } catch (Exception e) {
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TrackerMonitor exMonitor = new TrackerMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                sendMonitor.start();
                logger.error("retrying...... Exception:" + e.getMessage());
                delay(3);
            }
        }
    }

    private EntryPosition findPosFromMysqlNow() {
        EntryPosition returnPos = null;
        try {
            ResultSetPacket resultSetPacket = queryExecutor.query("show master status");
            List<String> fields = resultSetPacket.getFieldValues();
            if(CollectionUtils.isEmpty(fields)) {
                throw new Exception("show master status failed");
            }
            returnPos = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnPos;
    }

    private EntryPosition findPosFromMysqlNow(MysqlQueryExecutor executor) {
        if(executor == null) return null;
        EntryPosition pos = null;
        try {
            ResultSetPacket packet = executor.query("show master status");
            List<String> fields = packet.getFieldValues();
            if(CollectionUtils.isEmpty(fields)) {
                throw new Exception("show master status failed");
            }
            pos = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
        } catch (Exception e) {
            logger.error("show master status error!!!");
            e.printStackTrace();
        }
        return pos;
    }

    private EntryPosition findPosFromZk() {
        logger.info("finding position......");
        EntryPosition returnPos = null;
        try {
            String zkPos = config.persisPath + "/" + jobId;
            String getStr = zkExecutor.get(zkPos);
            if(getStr == null || getStr.equals("")) {
                if(offset <= 0) {
                    logger.info("find mysql show master status......");
                    returnPos = findPosFromMysqlNow();
                    batchId = 0;
                    inBatchId = 0;
                    logger.info("start position :" + returnPos.getBinlogPosFileName() + ":" + returnPos.getPosition() +
                            ":" + batchId +
                            ":" + inBatchId);
                    return returnPos;
                } else {
                    logger.info("find mysql position from configuration......");
                    returnPos = new EntryPosition(logfile, offset);
                    logger.info("start position :" + returnPos.getBinlogPosFileName() + ":" + returnPos.getPosition() +
                            ":" + batchId +
                            ":" + inBatchId);
                    return returnPos;
                }
            }
            String[] ss = getStr.split(":");
            if(ss.length != 4) {
                zkExecutor.delete(zkPos);
                logger.error("zk position format is error...");
                return null;
            }
            logger.info("find zk position......");
            returnPos = new EntryPosition(ss[0], Long.valueOf(ss[1]));
            batchId = Long.valueOf(ss[2]);
            inBatchId = Long.valueOf(ss[3]);
            logger.info("start position :" + returnPos.getBinlogPosFileName() + ":" + returnPos.getPosition() +
                    ":" + batchId +
                    ":" + inBatchId);
        } catch (Exception e) {
            logger.error("zk client error : " + e.getMessage());
            e.printStackTrace();
        }
        return returnPos;
    }

    public void prepare(String id) throws Exception {
        logger.info("preparing......");
        jobId = id;
        try {
            init();
        } catch (RetryTimesOutException e) {//reload the job by run()
            logger.error(e.getMessage());
            return;
        }
        //start thread
        fetchSurvival = true;
        fetcher.start();
        cptimer.schedule(confirm, TrackerConf.TIMER_TASK_DELAY, TrackerConf.CONFIRM_INTERVAL);
        htimer.schedule(heartBeat, TrackerConf.TIMER_TASK_DELAY, TrackerConf.HEARTBEAT_INTERVAL);
        //ip monitor
        //send monitor
        final String localIp = InetAddress.getLocalHost().getHostAddress();
        Thread sendMonitor = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    TrackerMonitor ipMonitor = new TrackerMonitor();
                    ipMonitor.ip = localIp;
                    JrdwMonitorVo jmv = ipMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.IP_MONITOR, jobId);
                    String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                    phMonitorSender.sendKeyMsg(km);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        sendMonitor.start();
        //log
        logger.info("start the tracker successfully......");
        logger.info("waiting prepare thread starting......");
        delay(5);//waiting threads start
    }

    class Fetcher extends Thread {
        private DirectLogFetcherChannel fetcher;
        private LogDecoder decoder;
        private LogContext context;
        private Logger logger = LoggerFactory.getLogger(Fetcher.class);
        private LogEvent event;
        private TrackerMonitor monitor = new TrackerMonitor();
        private TrackerMonitor minuteMonitor = new TrackerMonitor();
        public FetchMonitorMin timerMonitor = new FetchMonitorMin();
        public Timer timer = new Timer();
        public CanalEntry.Entry fetchLast;
        public LogEvent fetchLastEvent;

        public boolean iskilled = false;

        class FetchMonitorMin extends TimerTask {
            private Logger logger = LoggerFactory.getLogger(FetchMonitorMin.class);

            private long getRearNum(String str) {
                long ret = 0;
                for(int i = str.length() - 1; i >= 0; i--) {
                    if(!Character.isDigit(str.charAt(i))) {
                        String substr = str.substring(i + 1, str.length());
                        ret = Long.valueOf(substr);
                        break;
                    }
                }
                if(ret == 0) {
                    ret = Long.valueOf(str);
                }
                return ret;
            }

            private double getDelayNum(String logfile1, long pos1, String logfile2, long pos2) throws Exception {
                long filenum1 = getRearNum(logfile1);
                long filenum2 = getRearNum(logfile2);
                long subnum1 = filenum1 - filenum2;
                long subnum2 = pos1 - pos2;
                if(filenum2 == 0 || subnum2 == 0) {
                    return -1;//error position
                }
                if(subnum1 != 0) {
                    subnum2 = pos1;
                }
                if(subnum2 < 0) {
                    subnum2 = 0;
                }
                String s = subnum1 + "." +subnum2;
                double ret = Double.valueOf(s);
                return  ret;
            }

            public void run() {
                try {
                    logger.info(LOG_HEAD_LINE + "per minute fetch monitor:");
                    logger.info("---> fetch number of entry:" + minuteMonitor.fetchNum + " entries");
                    logger.info("---> fetch sum size :" + minuteMonitor.batchSize / config.mbUnit + " MB");
                    //set delay num monitor
                    EntryPosition pos = findPosFromMysqlNow(realQuery);
                    minuteMonitor.delayNum = 0;
                    if(fetchLast != null && pos != null) {
                        minuteMonitor.delayNum = getDelayNum(pos.getJournalName(), pos.getPosition(), eventConvert.getBinlogFileName(), fetchLastEvent.getLogPos());
                    } else {
                        minuteMonitor.delayNum = 0;
                    }
                    if(minuteMonitor.delayNum < 0) {
                        //no data then delay num is 0
                        minuteMonitor.delayNum = 0;
                    }
                    logger.info("---> fetch delay num :" + minuteMonitor.delayNum);
                    //send monitor phenix
                    JrdwMonitorVo jmv = minuteMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.FETCH_MONITOR, jobId);
                    JSONObject jmvObject = JSONConvert.JrdwMonitorVoToJson(jmv);
                    String jsonStr = jmvObject.toString();
                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                    phMonitorSender.sendKeyMsg(km);
                    minuteMonitor.clear();
                    logger.info("fetch monitor json: " + jsonStr);
                } catch (Exception e) {
                    logger.error("fetch monitor error: ##############" + e.getMessage(), e);
                }
            }
        }

        public void run() {
            try {
                init();
                int counter = 0;
                timer.schedule(timerMonitor, 1000, config.monitorsec * 1000);//start thread
                isFetchRunning = true;
                while (fetcher.fetch()) {
                    if (counter == 0) monitor.fetchStart = System.currentTimeMillis();
                    event = decoder.decode(fetcher, context);
                    if(event == null) {
                        logger.warn("fetched event is null...");
                        continue;
                    }
                    fetchLastEvent = event;
                    //entry to event
                    CanalEntry.Entry entry = eventConvert.parse(event);
                    if(entry == null) continue;
                    //add the entry to the queue
                    entryQueue.put(entry);
                    fetchLast = entry;
                    counter++;
                    minuteMonitor.fetchNum++;
                    monitor.batchSize += event.getEventLen();
                    minuteMonitor.batchSize += event.getEventLen();
                    if(counter >= config.batchsize) {//number / size | per minute
                        monitor.fetchEnd = System.currentTimeMillis();
                        logger.info(LOG_HEAD_LINE + "fetch thread : ");
                        logger.info("---> fetch during time : " + (monitor.fetchEnd - monitor.fetchStart) + " ms");
                        logger.info("---> fetch number : " + counter + " events");
                        logger.info("---> fetch sum size : " + monitor.batchSize / config.mbUnit + " MB");
                        monitor.clear();
                        counter = 0;
                    }
                    if(iskilled) break;
                }
            } catch (Throwable e) {
                logger.error("fetch thread encountered error : " + e.getMessage(), e);
                logger.info("fetch thread isKilled = " + iskilled);
                if(iskilled) return;
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TrackerMonitor exMonitor = new TrackerMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e1) {
                            logger.error("send exception to kafka failed, " + e1.getMessage(), e1);
                        }
                    }
                });
                sendMonitor.start();
                logger.error("fetch thread error : " + e.getMessage(), e);
                String errMsg = e.getMessage();
                if(errMsg.contains("errno = 1236")) {
                    try {
                        String zkPos = config.persisPath + "/" + jobId;
                        zkExecutor.delete(zkPos);//invalid position
                    } catch (Exception e1) {
                        logger.error(e1.getMessage(), e1);
                    }
                    globalFetchThread = 1;//reload
                    return;
                }
                if(errMsg.contains("zk position is error")) {
                    globalFetchThread = 1;//reload
                    return;
                }
                //all exception we will reload the job
                globalFetchThread = 1;
                return;
            }
            fetchSurvival = false;
        }

        private EntryPosition findHBaseCheckpoint() throws Exception {
            EntryPosition position = null;
            String value = cpUtil.readCheckpoint(jobId, CheckpointUtil.CURRENT_CHECKPOINT);
            if(value == null || value.equals("")) {
                throw new Exception("fetch checkpoint is null, reload the job " + jobId);
            }
            String vs[] = value.split(":");
            if(vs.length != 4) {
                throw new Exception("fetch checkpoint's format is error, checkpoint = " + value + ", reload the job " + jobId);
            }
            position = new EntryPosition(vs[0], Long.valueOf(vs[1]));
            batchId = Long.valueOf(vs[2]);
            inBatchId = Long.valueOf(vs[3]);
            logger.info(LOG_HEAD_LINE + "checkpoint load :");
            logger.info("---------------> start position :" + position.getBinlogPosFileName() + ":" + position.getPosition() +
                    ":" + batchId +
                    ":" + inBatchId);
            return position;
        }

        private void init() throws Exception {
            //find start position
            //EntryPosition startPos = findHBaseCheckpoint();
            EntryPosition startPos = findPosFromZk();
            if(startPos == null) throw new Exception("fetch position is error...");
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
            binDmpPacket.binlogFileName = startPos.getJournalName();
            binDmpPacket.binlogPosition = startPos.getPosition();
            binDmpPacket.slaveServerId = config.slaveId;
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(logConnector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            //initialize the mysql.dbsync to fetch the binlog data
            fetcher = new DirectLogFetcherChannel(logConnector.getReceiveBufferSize());
            fetcher.start(logConnector.getChannel());
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }

        public void shutdown() {
            timerMonitor.cancel();
            timer.cancel();
        }
    }

    class HeartBeat extends TimerTask {
        private Logger logger = LoggerFactory.getLogger(HeartBeat.class);

        public void run() {

            try {

                logger.info(LOG_HEAD_LINE + "check assembly heartbeats......");

                //run function heartbeat
                logger.info("-------> globalFetchThread :" + globalFetchThread);
                logger.info("-------> size of entryQueue :" + entryQueue.size());
                logger.info("-------> fetch thread status :" + fetcher.getState());
                logger.info("-------> queue size = " + entryQueue.size());

                //check mysql connection heartbeat
                if(!logConnector.isConnected() || !tableConnector.isConnected() || !realConnector.isConnected()) {
                    logger.error("mysql connection loss, reload the job ......");
                    globalFetchThread = 1;
                    return;
                }
                //check fetch survival
                if(!fetchSurvival) {
                    logger.error("fetch thread had been dead, reload the job ......");
                    globalFetchThread = 1;
                    return;
                }
                if(!fetcher.isAlive()) {
                    logger.error("fetch thread is not alive, reloading the job ......");
                    globalFetchThread = 1;
                    return;
                }
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                logger.info("heartbeat encountered error, reloading job......");
                globalFetchThread = 1;
            }
        }
    }

    /*confirm cp per minute*/
    class ConfirmCP extends TimerTask {

        /*logger*/
        private Logger logger = LoggerFactory.getLogger(ConfirmCP.class);

        /**
         * confirm the checkpoint in a fixed time internal
         */
        @Override
        public void run() {
            /*logger*/
            logger.info(LOG_HEAD_LINE + "confirm checkpoint every minute:");
            /*confirm the checkpoint*/
            int count = 0;
            while (count < TrackerConf.CP_RETRY_COUNT) {
                try {
                    if (!StringUtils.isBlank(ConCP)) {
                        //cpUtil.writeCp(jobId, ConCP);
                        writeCPZk(jobId, ConCP);
                        logger.info("----------> confirm checkpoint = " + ConCP);
                    } else {
                        logger.info("----------> nothing to confirm checkpoint, checkpoint is null");
                    }
                    break;//if send success
                } catch (Throwable e) {
                    logger.error("confirm minuter is dead, error = " + e.getMessage(), e);
                    count++;
                }
            }
            if(count >= TrackerConf.CP_RETRY_COUNT) {
                logger.error("confirm checkpoint failed by " + count + " times, reloading the job......");
                globalFetchThread = 1;
            }
        }
    }

    private void writeCPZk(String jobId, String cp) throws Exception {
        String path = config.persisPath + "/" + jobId;
        String value = cp;
        if(!zkExecutor.exists(path))
            zkExecutor.create(path, value);
        else
            zkExecutor.set(path, value);
    }

    public void run() throws Exception {

        //heartbeat thread is dead, reloading immediately


        //cpu 100%
        if(entryQueue.isEmpty()) {
            delayMin(100);
        }

        //check fetch thread status
        if(globalFetchThread == 1) {
            globalFetchThread = 0;
            logger.error("connect loss or position is error!!! reload......");
            reload(jobId);
            delay(5);
            return;
        }

        //waiting for prepare finish and prepare's thread finish or run onece
        if(!isFetchRunning){
            delay(3);
            return;
        }

        //while heart time
        int continuousFiltered = 0;
        int continuousZero = 0;

        //take the data from the queue
        while (!entryQueue.isEmpty()) {
            CanalEntry.Entry entry = entryQueue.take();
            if(entry == null) continue;
            //only persist the xid event (that is to say commit event)
            //think about that a whole batch events is send but there is no xid event in this batch
            //  we do not persist cp in this batch until there is a xid event in later batch events
            //  if restart tracker, so we send repeated data, but parser can filter it by batchId and inBatchId (sequence id)
            if(isEndCommitEntry(entry))
                lastEntry = entry;
            String dbtb = entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
            if(isInMap(dbtb)) {
                continuousFiltered = 0;
                // re-pack the entry
                entry =
                        CanalEntry.Entry.newBuilder()
                                .setHeader(entry.getHeader())
                                .setEntryType(entry.getEntryType())
                                .setStoreValue(entry.getStoreValue())
                                .setBatchId(batchId)
                                .setInId(inBatchId)
                                .setIp(config.address)
                                .build();
                inBatchId++;//batchId.inId almost point next event's position
                if(isEndEntry(entry)) {// instead of isEndEntry(entry)
                    inBatchId = 0;
                    batchId++;
                }
                byte[] value = entry.toByteArray();
                monitor.batchSize += value.length;
                KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.topic, null, value);
                messageList.add(km);
            } else {
                continuousFiltered++;
                if(continuousFiltered >= TrackerConf.FILTER_NUM_LOGGER) {
                    logger.info(LOG_HEAD_LINE + "filtered entries:");
                    logger.info("----> filtered num = " + continuousFiltered);
                    logger.info("----> last filtered entry dbname.tbname = " + dbtb);
                    continuousFiltered = 0;//reset
                }
            }
            if(monitor.batchSize >= config.sendBytes || messageList.size() >= config.batchsize) break;//check per batch size
        }
        //per minute record
//        if(lastEntry != null) {
//            binlog = lastEntry.getHeader().getLogfileName();
//            globalBinlogName = binlog;
//            globalXidEntry = lastEntry;
//            globalXidBatchId = batchId;
//            globalXidInBatchId = inBatchId;
//        }
        // serialize the list -> filter -> batch for it -> send the batched bytes to the kafka; persistence the batched list???
        // or no batched list???
        // I got it : mysqlbinlog:pos could be no filtered event but batchId and inBatchId must be filtered event
        //     so the mysqlbinlog:pos <--> batchId:inBatchId Not must be same event to same event
        // mysqlbinlog:pos <- no filter list's xid  batchid:inBatchId <- filter list's last event
        //entryList data to kafka , per time must confirm the position
        if((monitor.batchSize >= config.sendBytes || messageList.size() >= config.batchsize) || (System.currentTimeMillis() - startTime) > config.sendTimeInterval ) {
            //if(lastEntry == null) return; // not messageList but entryList or lastEntry , when we fetched not filtered data , we also confirm the position for it
            if(messageList.size() > 0) {
                monitor.persisNum = messageList.size();
                monitor.delayTime = (System.currentTimeMillis() - lastEntry.getHeader().getExecuteTime());
            }
            if(persisteKeyMsg(messageList) == -1) {
                logger.info("persistence the data and retry, reconn failed !!! reloading ......");
                globalFetchThread = 1;
                return;
            }
            if(messageList.size() > 0) {
                confirm2CpStr(lastEntry);
                continuousZero = 0;//reset
            } else {
                continuousZero++;
                if(continuousZero >= TrackerConf.CONTINUOUS_ZERO_NUM) {
                    logger.info(LOG_HEAD_LINE + "continuous running 0 batch :");
                    logger.info("-------> 0 batch count = " + continuousZero);
                    logger.info("-------> sleeping " + TrackerConf.SLEEPING_RUNNING + " ms");
                    Thread.sleep(TrackerConf.SLEEPING_RUNNING);
                    continuousZero = 0;//reset
                }
            }
            //per minute record
            if(lastEntry != null) {
                binlog = lastEntry.getHeader().getLogfileName();
                globalBinlogName = binlog;
                globalXidEntry = lastEntry;
                globalXidBatchId = batchId;
                globalXidInBatchId = inBatchId;
            }
            messageList.clear();
        }
        if(monitor.persisNum > 0) {
            monitor.persistenceStart = startTime;
            monitor.persistenceEnd = System.currentTimeMillis();
            logger.info(LOG_HEAD_LINE + "persistence thread / monitor:");
            logger.info("---> persistence deal during time:" + (monitor.persistenceEnd - monitor.persistenceStart) + " ms");
            logger.info("---> send time :" + (monitor.sendEnd - monitor.sendStart) + " ms");
            logger.info("---> parser delay time:" + monitor.delayTime + " ms");
            logger.info("---> the number of entry list: " + monitor.persisNum  + " entries");
            logger.info("---> entry list to bytes sum size is " + monitor.batchSize / config.mbUnit + " MB");
            logger.info("---> commit position info (not confirmed):" + " binlog file is " + globalBinlogName +
                    ",position is :" + (lastEntry.getHeader().getLogfileOffset() + lastEntry.getHeader().getEventLength()) + "; batch id is :" + batchId +
                    ",in batch id is :" + inBatchId);
            //send phoenix monitor
            final TrackerMonitor phMonitor = monitor.cloneDeep();
            Thread sendMonitor = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JrdwMonitorVo jmv = phMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.PERSIS_MONITOR, jobId);
                        String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                        KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                        phMonitorSender.sendKeyMsg(km);
                        logger.info("monitor persistence:" + jsonStr);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            sendMonitor.start();
            monitor.clear();
            startTime = System.currentTimeMillis();
        }
    }

    private boolean isInMap(String key) {
        return config.filterMap.containsKey(key);
    }

    private void persisteData(List<CanalEntry.Entry> entries) {
        monitor.persistenceStart = System.currentTimeMillis();
        List<byte[]> bytesList = new ArrayList<byte[]>();
        for(CanalEntry.Entry entry : entries) {
            byte[] value = entry.toByteArray();
            bytesList.add(value);
            monitor.batchSize += value.length;
        }
        monitor.persistenceEnd = System.currentTimeMillis();
        monitor.hbaseWriteStart = System.currentTimeMillis();
        if(bytesList.size() > 0) {
            msgSender.send(bytesList);
        }
        monitor.hbaseWriteEnd = System.currentTimeMillis();

    }
    //number / size / yanshi / send kafka time(now - last event of list) | per minute
    private int persisteKeyMsg(List<KeyedMessage<String, byte[]>> msgs) {
        if(msgs.size() == 0) return 0;
        monitor.sendStart = System.currentTimeMillis();
        int flag = msgSender.sendKeyMsg(msgs, phMonitorSender, config);
        monitor.sendEnd = System.currentTimeMillis();
        return flag;
    }

    private void confirmPos(LogEvent last, String bin) throws Exception {
        if(last != null) {
            String pos = bin + ":" + last.getLogPos() + ":" + batchId + ":" + inBatchId;
            try {
                String zkPos = config.persisPath + "/" + jobId;
                if(!zkExecutor.exists(zkPos)) {
                    zkExecutor.create(zkPos, pos);
                } else {
                    zkExecutor.set(zkPos, pos);
                }
            } catch (Exception e) { //retry
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TrackerMonitor exMonitor = new TrackerMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                sendMonitor.start();
                logger.error(e.getMessage());
                boolean isconn = false;
                int isZk = 0;
                while (!isconn) {
                    if(isZk >= config.retrys) {
                        globalFetchThread = 1;
                        return;
                    }
                    isZk++;
                    try {
                        String zkpos = config.persisPath + "/" + jobId;
                        zkExecutor.set(zkpos, pos);
                        isconn = true;
                    } catch (Exception e1) {
                        //send monitor
                        final String exmsg1 = e1.getMessage();
                        Thread sendMonitor1 = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    TrackerMonitor exMonitor = new TrackerMonitor();
                                    exMonitor.exMsg = exmsg1;
                                    JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                                    String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                                    phMonitorSender.sendKeyMsg(km);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        sendMonitor1.start();
                        logger.error("retrying...... Exception:" +e1.getMessage());
                        delay(3);
                    }
                }
            }
        }
    }

    private void confirmHBasePos(CanalEntry.Entry entry) throws Exception {
        if(entry != null) {
            String bin = entry.getHeader().getLogfileName();
            String pos = bin + ":" + (entry.getHeader().getLogfileOffset() + entry.getHeader().getEventLength()) + ":" + batchId + ":" + inBatchId;
            cpUtil.writeCp(jobId, pos);
        }
    }

    /**
     * confirm to a variable temporarily
     * @param entry, transaction end entry
     * @throws Exception
     */
    private void confirm2CpStr(CanalEntry.Entry entry) throws Exception {
        if(entry != null) {
            String bin = entry.getHeader().getLogfileName();
            String pos = bin + ":" + (entry.getHeader().getLogfileOffset() + entry.getHeader().getEventLength()) + ":" + batchId + ":" + inBatchId;
            ConCP = pos;
        }
    }

    private void confirmPos(CanalEntry.Entry entry) throws Exception {
        if(entry != null) {
            String bin = entry.getHeader().getLogfileName();
            String pos = bin + ":" + (entry.getHeader().getLogfileOffset() + entry.getHeader().getEventLength()) + ":" + batchId + ":" + inBatchId;
            try {
                String zkPos = config.persisPath + "/" + jobId;
                if(!zkExecutor.exists(zkPos)) {
                    zkExecutor.create(zkPos, pos);
                } else {
                    zkExecutor.set(zkPos, pos);
                }
            } catch (Exception e) { //retry
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TrackerMonitor exMonitor = new TrackerMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                sendMonitor.start();
                logger.error(e.getMessage());
                boolean isconn = false;
                int isZk = 0;
                while (!isconn) {
                    if(isZk >= config.retrys) {
                        globalFetchThread = 1;
                        return;
                    }
                    isZk++;
                    try {
                        String zkpos = config.persisPath + "/" + jobId;
                        zkExecutor.set(zkpos, pos);
                        isconn = true;
                    } catch (Exception e1) {
                        //send monitor
                        final String exmsg1 = e1.getMessage();
                        Thread sendMonitor1 = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    TrackerMonitor exMonitor = new TrackerMonitor();
                                    exMonitor.exMsg = exmsg1;
                                    JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlTrackerMonitorType.EXCEPTION_MONITOR, jobId);
                                    String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                                    phMonitorSender.sendKeyMsg(km);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        sendMonitor1.start();
                        logger.error("retrying...... Exception:" +e1.getMessage());
                        delay(3);
                    }
                }
            }
        }
    }

    private void confirmPos(CanalEntry.Entry entry, String bin) throws Exception {
        if(entry != null) {
            String pos = bin + ":" + (entry.getHeader().getLogfileOffset() + entry.getHeader().getEventLength()) + ":" + batchId + ":" + inBatchId;
            try {
                String zkPos = config.persisPath + "/" + jobId;
                if(!zkExecutor.exists(zkPos)) {
                    zkExecutor.create(zkPos, pos);
                } else {
                    zkExecutor.set(zkPos, pos);
                }
            } catch (Exception e) { //retry
                logger.error(e.getMessage());
                boolean isconn = false;
                int isZk = 0;
                while (!isconn) {
                    if(isZk >= config.retrys) {
                        globalFetchThread = 1;
                        return;
                    }
                    isZk++;
                    try {
                        String zkpos = config.persisPath + "/" + jobId;
                        zkExecutor.set(config.persisPath, pos);
                        isconn = true;
                    } catch (Exception e1) {
                        logger.error("retrying...... Exception:" +e1.getMessage());
                        delay(3);
                    }
                }
            }
        }
    }

    @Deprecated
    private boolean isEndEvent(LogEvent event){
        if((event.getHeader().getType()==LogEvent.XID_EVENT)
                ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN"))){
            return (true);
        }
        else    return(false);
    }

    //maybe bug because of getisddl() best is !(BEGIN || COMMIT)
    @Deprecated
    private boolean isEndEntry(CanalEntry.Entry entry) {
        try {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) return true;
            CanalEntry.RowChange rc = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            if(rc.getIsDdl() && entry.getEntryType() == CanalEntry.EntryType.ROWDATA) return true;
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean isEndCommitEntry(CanalEntry.Entry entry) {
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND)
            return true;
        else
            return false;
    }

    public void pause(String id) throws Exception {

    }

    public void reload(String id) throws Exception {
        close(jobId);
        prepare(jobId);
    }

    public void close(String id) throws Exception {
        logger.info("closing the job......");
        if(fetcher != null) {
            fetcher.iskilled = true;//stop the fetcher thread
            fetcher.shutdown();//stop the fetcher's timer task
        }
        if(heartBeat != null)
            heartBeat.cancel();//stop the heart beat thread
        if(cptimer != null)
            cptimer.cancel();
        if(htimer != null)
            htimer.cancel();
        if(logConnector != null)
            logConnector.disconnect();
        if(realConnector != null)
            realConnector.disconnect();
        if(tableConnector != null)
            tableConnector.disconnect();
        if(msgSender != null)
            msgSender.close();
        if(zkExecutor != null)
            zkExecutor.close();
        if(config != null)
            config.clear();
        logger.info("system exiting......");
        System.exit(0);
    }

    class RetryTimesOutException extends Exception {
        public RetryTimesOutException(String msg) {
            super(msg);
        }
    }
}
