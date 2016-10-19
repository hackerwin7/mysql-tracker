package com.github.hackerwin7.mysql.tracker.tracker.parser;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.*;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.SimpleEntry;
import com.github.hackerwin7.mysql.tracker.tracker.common.CanalParseException;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMeta;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2016/02/23
 * Time: 5:24 PM
 * Desc: simply convert event to simple entry
 * Tips:
 */
public class SimpleLogEventConvert {
    public static final String          ISO_8859_1          = "ISO-8859-1";
    public static final String          UTF_8               = "UTF-8";
    public static final int             TINYINT_MAX_VALUE   = 256;
    public static final int             SMALLINT_MAX_VALUE  = 65536;
    public static final int             MEDIUMINT_MAX_VALUE = 16777216;
    public static final long            INTEGER_MAX_VALUE   = 4294967296L;
    public static final BigInteger      BIGINT_MAX_VALUE    = new BigInteger("18446744073709551616");
    public static final int             version             = 1;
    public static final String          BEGIN               = "BEGIN";
    public static final String          COMMIT              = "COMMIT";
    public static final Logger          logger              = LoggerFactory.getLogger(LogEventConvert.class);

    private TableMetaCache              tableMetaCache      = null;

    private String                      binlogFileName      = "mysql-bin.000001";
    private Charset                     charset             = Charset.defaultCharset();

    //additional info
    public static Map<String, String>   filterMap           = new HashMap<String, String>();

    public String getBinlogFileName() {
        return binlogFileName;
    }

    /**
     * parse event to entry
     * @param logEvent
     * @return simple entry
     * @throws Exception
     */
    public SimpleEntry.Entry parse(LogEvent logEvent) throws Exception {
        if (logEvent == null || logEvent instanceof UnknownLogEvent) {
            return null;
        }

        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                logger.info("EVENT : rotate");
                binlogFileName = ((RotateLogEvent) logEvent).getFilename();
                break;
            case LogEvent.QUERY_EVENT:
                logger.info("EVENT : query");
                //do not transfer query event
                //return parseQueryEvent((QueryLogEvent) logEvent);
                break;
            case LogEvent.XID_EVENT:
                logger.info("EVENT : xid");
                return parseXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                logger.info("EVENT : table_map");
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
                logger.info("EVENT : write_rows");
                return parseRowsEvent((WriteRowsLogEvent) logEvent);
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.UPDATE_ROWS_EVENT:
                logger.info("EVENT : update_rows");
                return parseRowsEvent((UpdateRowsLogEvent) logEvent);
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                logger.info("EVENT : delete_rows");
                return parseRowsEvent((DeleteRowsLogEvent) logEvent);
            default:
                break;
        }

        return null;
    }

    /**
     * parse event to transaction begin or end entry
     * @param event
     * @return entry
     */
    private SimpleEntry.Entry parseQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, SimpleEntry.EntryType.TRANSACTIONBEGIN, null);
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, SimpleEntry.EntryType.TRANSACTIONEND, null);
        } else {
            // ddl
            // no op
            return null;
        }
    }

    /**
     * parse to transaction end
     * @param event
     * @return entry
     */
    private SimpleEntry.Entry parseXidEvent(XidLogEvent event) {
        SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
        return createEntry(header, SimpleEntry.EntryType.TRANSACTIONEND, null);
    }

    /**
     * rowdata event parse
     * @param event
     * @return row data entry
     * @throws Exception
     */
    private SimpleEntry.Entry parseRowsEvent(RowsLogEvent event) throws Exception {
        //filter dbtb
        TableMapLogEvent table = event.getTable();
        if(table == null)
            throw new Exception("not found tableId: " + event.getTableId());
        String dbName = table.getDbName();
        String tbName = table.getTableName();
        String dbtb = dbName + "." + tbName;
        //customer filter
        if(filterMap.size() > 0 && !filterMap.containsKey(dbtb))
            return null;
        SimpleEntry.EventType eventType = null;
        int type = event.getHeader().getType();
        if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
            eventType = SimpleEntry.EventType.INSERT;
        } else if (LogEvent.UPDATE_ROWS_EVENT_V1 == type || LogEvent.UPDATE_ROWS_EVENT == type) {
            eventType = SimpleEntry.EventType.UPDATE;
        } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
            eventType = SimpleEntry.EventType.DELETE;
        } else {
            throw new CanalParseException("unsupport event type :" + event.getHeader().getType());
        }
        //build header
        SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), dbName, tbName, eventType);
        //build rowchange (row data)
        SimpleEntry.RowChange.Builder rowChangeBuilder = SimpleEntry.RowChange.newBuilder();
        RowsLogBuffer buffer = event.getRowsBuf(charset.name());
        BitSet columns = event.getColumns();
        BitSet changeColumns = event.getColumns();
        TableMeta tableMeta = null;
        if(tableMetaCache != null) {
            tableMeta = tableMetaCache.getTableMeta(dbName, tbName);
            if(tableMeta == null)
                throw new Exception("not found [" + dbtb + "] in db");
        }
        while (buffer.nextOneRow(columns)) {
            SimpleEntry.RowData.Builder rowDataBuilder = SimpleEntry.RowData.newBuilder();
            if(SimpleEntry.EventType.INSERT == eventType)
                parseOneRow(rowDataBuilder, event, buffer, columns, true, tableMeta);
            else if(SimpleEntry.EventType.DELETE == eventType)
                parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta);
            else {
                parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta);
                if(!buffer.nextOneRow(changeColumns)) {
                    rowChangeBuilder.addRowData(rowDataBuilder.build());
                    break;
                }
                parseOneRow(rowDataBuilder, event, buffer, event.getChangeColumns(), true, tableMeta);
            }
            rowChangeBuilder.addRowData(rowDataBuilder.build());
        }
        return createEntry(header, SimpleEntry.EntryType.ROWDATA, rowChangeBuilder.build());
    }

    /**
     * parse one row data
     * @param rowDataBuilder
     * @param event
     * @param buffer
     * @param cols
     * @param isAfter
     * @param tableMeta
     * @throws UnsupportedEncodingException
     */
    private void parseOneRow(SimpleEntry.RowData.Builder rowDataBuilder, RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter, TableMeta tableMeta) throws Exception {
        final int columnCnt = event.getTable().getColumnCnt();
        final TableMapLogEvent.ColumnInfo[] columnInfos = event.getTable().getColumnInfo();
        //refresh table meta info
        if(tableMeta != null && columnInfos.length > tableMeta.getFileds().size()) {
            tableMeta = tableMetaCache.getTableMeta(event.getTable().getDbName(), event.getTable().getTableName(), false);
            if(tableMeta == null)
                throw new Exception("not found [" + event.getTable().getDbName() + "." + event.getTable().getTableName() + "] in mysql");
            if(tableMeta != null && columnInfos.length > tableMeta.getFileds().size())
                throw new Exception("column size is not match for table:" + tableMeta.getFullName() + "," + columnInfos.length + " != " + tableMeta.getFileds().size());
        }
        //build column
        for(int i = 0; i <= columnCnt - 1; i++) {
            TableMapLogEvent.ColumnInfo info = columnInfos[i];
            SimpleEntry.Column.Builder columnBuilder = SimpleEntry.Column.newBuilder();

            TableMeta.FieldMeta fieldMeta = null;
            if(tableMeta != null) {
                fieldMeta = tableMeta.getFileds().get(i);
                columnBuilder.setName(fieldMeta.getColumnName());
                columnBuilder.setIsKey(fieldMeta.isKey());
            }
            columnBuilder.setIsNull(false);
            boolean isBinary = false;
            if(fieldMeta != null) {
                if(StringUtils.containsIgnoreCase(fieldMeta.getColumnType(), "VARBINARY"))
                    isBinary = true;
                else if(StringUtils.containsIgnoreCase(fieldMeta.getColumnType(), "BINARY"))
                    isBinary = true;
            }
            buffer.nextValue(info.type, info.meta, isBinary);

            int javaType = buffer.getJavaType();
            if(buffer.isNull()) {
                columnBuilder.setIsNull(true);
            } else {
                final Serializable value = buffer.getValue();
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        Number number = (Number) value;
                        if(fieldMeta != null && fieldMeta.isUnsigned() && number.longValue() < 0) {
                            switch (buffer.getLength()) {
                                case 1:
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(TINYINT_MAX_VALUE + number.intValue())));
                                    javaType = Types.SMALLINT;
                                    break;
                                case 2:
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(SMALLINT_MAX_VALUE + number.intValue())));
                                    javaType = Types.INTEGER;
                                    break;
                                case 3:
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(MEDIUMINT_MAX_VALUE + number.intValue())));
                                    javaType = Types.INTEGER;
                                    break;
                                case 4:
                                    columnBuilder.setValue(String.valueOf(Long.valueOf(INTEGER_MAX_VALUE + number.longValue())));
                                    javaType = Types.BIGINT;
                                    break;
                                case 8:
                                    columnBuilder.setValue(BIGINT_MAX_VALUE.add(BigInteger.valueOf(number.longValue())).toString());
                                    javaType = Types.DECIMAL;
                                    break;
                            }
                        } else {
                            columnBuilder.setValue(String.valueOf(value));
                        }
                        break;
                    case Types.REAL:
                    case Types.DOUBLE:
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.BIT:
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.DECIMAL:
                        columnBuilder.setValue(((BigDecimal) value).toPlainString());
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIME:
                    case Types.DATE:
                        columnBuilder.setValue(value.toString());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        if(fieldMeta != null && isText(fieldMeta.getColumnType())) {
                            columnBuilder.setValue(new String((byte[]) value, charset));
                            javaType = Types.CLOB;
                        } else {
                            columnBuilder.setValue(new String((byte[]) value, ISO_8859_1));
                            javaType = Types.BLOB;
                        }
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        columnBuilder.setValue(value.toString());
                        break;
                    default:
                        columnBuilder.setValue(value.toString());
                }
            }
            columnBuilder.setIsUpdated(isAfter && isUpdate(rowDataBuilder.getBeforeColumnsList(), columnBuilder.getIsNull() ? null : columnBuilder.getValue(), i));
            if(isAfter)
                rowDataBuilder.addAfterColumns(columnBuilder.build());
            else
                rowDataBuilder.addBeforeColumns(columnBuilder.build());
        }
    }

    /**
     * judge the text type
     * @param columnType
     * @return bool
     */
    private boolean isText(String columnType) {
        return "LONGTEXT".equalsIgnoreCase(columnType) || "MEDIUMTEXT".equalsIgnoreCase(columnType)
                || "TEXT".equalsIgnoreCase(columnType) || "TINYTEXT".equalsIgnoreCase(columnType);
    }

    /**
     * compare the after value with the before value
     * @param bfColumns
     * @param newValue
     * @param index
     * @return bool
     */
    private boolean isUpdate(List<SimpleEntry.Column> bfColumns, String newValue, int index) throws Exception {
        if(bfColumns == null) {
            throw new Exception("before value is null.");
        }

        if(index < 0)
            return false;
        if((bfColumns.size() - 1) < index)
            return false;
        SimpleEntry.Column column = bfColumns.get(index);

        if(column.getIsNull()) {
            if(newValue != null)
                return true;
            else
                return false;
        } else {
            if(newValue == null)
                return true;
            else
                return !column.getValue().equals(newValue);
        }
    }

    /**
     * create simple entry header
     * @param binlogFile
     * @param logHeader
     * @param dbName
     * @param tbName
     * @param eventType
     * @return header
     */
    private SimpleEntry.Header createHeader(String binlogFile, LogHeader logHeader, String dbName, String tbName, SimpleEntry.EventType eventType) {
        SimpleEntry.Header.Builder headerBuilder = SimpleEntry.Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setTs(logHeader.getWhen() * 1000L);
        headerBuilder.setEventLen(logHeader.getEventLen());
        if(eventType != null)
            headerBuilder.setEventType(eventType);
        if(!StringUtils.isBlank(dbName))
            headerBuilder.setDatabaseName(dbName);
        if(!StringUtils.isBlank(tbName))
            headerBuilder.setTableName(tbName);
        return headerBuilder.build();
    }

    /**
     * build the entry
     * @param header
     * @param entryType
     * @param rowChange
     * @return entry
     */
    private SimpleEntry.Entry createEntry(SimpleEntry.Header header, SimpleEntry.EntryType entryType, SimpleEntry.RowChange rowChange) {
        SimpleEntry.Entry.Builder entryBuilder = SimpleEntry.Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        if(rowChange != null)
            entryBuilder.setRowChange(rowChange);
        return entryBuilder.build();
    }

    /* setter */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void setTableMetaCache(TableMetaCache tableMetaCache) {
        this.tableMetaCache = tableMetaCache;
    }
}
