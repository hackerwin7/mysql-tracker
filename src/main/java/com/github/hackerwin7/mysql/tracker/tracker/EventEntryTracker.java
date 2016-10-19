package com.github.hackerwin7.mysql.tracker.tracker;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogContext;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogDecoder;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.server.ResultSetPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.tracker.tracker.parser.LogEventConvert;
import com.github.hackerwin7.mysql.tracker.tracker.position.EntryPosition;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fff on 11/8/15.
 */
public class EventEntryTracker {
    //static
    private static  String addr = "192.168.144.116";
    private static  int port = 3306;
    private static  String username = "canal";
    private static  String password = "canal";
    private static  long slaveId = 9876;

    private static String logFileName = "mysql-bin.000005";
    private static long logFilePos = 275L;

    private Map<String, String> filter = new HashMap<String, String>();

    private Logger logger = LoggerFactory.getLogger(EventEntryTracker.class);
    private MysqlConnector connector;
    private MysqlConnector connectorTable;
    private MysqlQueryExecutor queryExecutor;
    private MysqlUpdateExecutor updateExecutor;
    private EntryPosition startPosition;
    private TableMetaCache tableMetaCache;
    private LogEventConvert eventParser;
    private DirectLogFetcherChannel fetcher;
    private LogDecoder decoder;
    private LogContext context;
    public String CLASS_PREFIX = "classpath:";

    private void preDump() throws Exception {
        logger.info("prepare dump mysql......");
        connector = new MysqlConnector(new InetSocketAddress(addr, port), username, password);
        connectorTable = new MysqlConnector(new InetSocketAddress(addr, port), username, password);
        connector.connect();
        connectorTable.connect();
        queryExecutor = new MysqlQueryExecutor(connector);
        updateExecutor = new MysqlUpdateExecutor(connectorTable);
        logger.info("finding start position......");
        if(logFileName == null) {
            startPosition = findStartPosition();
        } else {
            startPosition = new EntryPosition(logFileName, logFilePos);
        }
        tableMetaCache = new TableMetaCache(connectorTable);
        eventParser = new LogEventConvert();
        eventParser.setTableMetaCache(tableMetaCache);
        eventParser.filterMap.putAll(filter);
        logger.info("filter = " + filter + ", size = " + filter.size());
        logger.info("start position = " + startPosition.getBinlogPosFileName() + ":" + startPosition.getPosition());
    }

    private EntryPosition findStartPosition() throws IOException {
        ResultSetPacket resultSetPacket = queryExecutor.query("show master status");
        List<String> fields = resultSetPacket.getFieldValues();
        if(CollectionUtils.isEmpty(fields)) {
            throw new NullPointerException("show master status failed!");
        }
        return new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
    }

    private void binlogDump() throws Exception {
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
        binDmpPacket.slaveServerId = slaveId;
        byte[] dmpBody = binDmpPacket.toBytes();
        HeaderPacket dmpHeader = new HeaderPacket();
        dmpHeader.setPacketBodyLength(dmpBody.length);
        dmpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(connector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
        //initialize the mysql.dbsync to fetch the binlog data
        fetcher = new DirectLogFetcherChannel(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        context = new LogContext();
        while (fetcher.fetch()) {
            LogEvent event = decoder.decode(fetcher, context);
            if(event == null) {
                logger.error("event is null!!");
                return;
            }
            //printEvent(event);
            CanalEntry.Entry entry = eventParser.parse(event);
            //show log info
            printEE(event, entry);
        }
    }

    private void printEE(LogEvent event, CanalEntry.Entry entry) throws Exception {
        logger.info("-----------------------------------> event : " + event);
        logger.info("event position = " + event.getLogPos());
        logger.info("event length = " + event.getEventLen());
        logger.info("event type = " + getMyType(event.getHeader().getType()));
        logger.info("===================================> entry : " + entry);
    }

    private String getMyType(int typeId) throws Exception {
        return LogEvent.getTypeName(typeId);
    }

    private void printEvent(LogEvent event) throws Exception {
        CanalEntry.Entry entry = eventParser.parse(event);
        if(entry == null) {
            logger.info("null entry!!!");
            return;
        }
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        if(rowChange.getIsDdl()) {
            logger.info("--------------------------------------------------entry----------------------------------------------------");
            logger.info("ddl : " + rowChange.getSql());
            logger.info("event time :" + entry.getHeader().getExecuteTime());
        } else if(entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            logger.info("--------------------------------------------------entry----------------------------------------------------");
            logger.info("dml : " + rowChange.getSql());
            logger.info("event time : " + entry.getHeader().getExecuteTime());
            logger.info("====================== rowdata ==============");
            for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if(rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                    List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
                    for (CanalEntry.Column column : columns) {
                        logger.info(column.getName() + ":" + column.getValue());
                    }
                } else if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                    List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                    for (CanalEntry.Column column : columns) {
                        logger.info(column.getName() + ":" + column.getValue());
                    }
                } else if ((rowChange.getEventType() == CanalEntry.EventType.UPDATE)) {
                    List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                    for (CanalEntry.Column column : columns) {
                        logger.info(column.getName() + ":" + column.getValue());
                    }
                }
            }
        } else {
            return;
        }
        logger.info("---------- summary -------");
        logger.info("dbname.tbname : " + entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName());
        logger.info("position : " + entry.getHeader().getLogfileName() + "#" + entry.getHeader().getLogfileOffset());
    }

    public void start() throws Exception {
        preDump();
        binlogDump();
    }

    public static void main(String[] args) throws Exception {
        EventEntryTracker tracker = new EventEntryTracker();
        tracker.start();
    }
}
