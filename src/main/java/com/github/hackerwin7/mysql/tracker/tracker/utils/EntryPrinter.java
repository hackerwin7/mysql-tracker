package com.github.hackerwin7.mysql.tracker.tracker.utils;

import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Date;
import java.util.List;

/**
 * Created by hp on 14-9-3.
 */
public class EntryPrinter {

    private CanalEntry.Entry entry;

    public EntryPrinter(CanalEntry.Entry entry) {
        this.entry = entry;
    }


    public static void printEntrys(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    System.out.println("============================================================================> TRANSACTIONBEGIN EVENT :");
                    System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                    System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                    System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                    System.out.println("delay time:"+String.valueOf(delayTime));
                    System.out.println("BEGIN ----> Thread id:"+begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    System.out.println("============================================================================> TRANSACTIONEND EVENT :");
                    System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                    System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                    System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                    System.out.println("delay time:"+String.valueOf(delayTime));
                    System.out.println("END ----> transaction id:"+end.getTransactionId());
                }

                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();
                //print the row information
                System.out.println("============================================================================> ROWDATA EVENT :");
                System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                System.out.println("schema name:"+entry.getHeader().getSchemaName());
                System.out.println("table name:"+entry.getHeader().getTableName());
                System.out.println("event type:"+eventType);
                System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                System.out.println("delay time:"+String.valueOf(delayTime));

                if (rowChage.getIsDdl()) {
                    System.out.println("SQL ----> " + rowChage.getSql());
                    //continue;
                }

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {//update
                        //before
                        printColumn(rowData.getBeforeColumnsList());
                        //after
                        printColumn(rowData.getAfterColumnsList());
                        //updated
                        printColumn(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    public static void printEntry(CanalEntry.Entry entry){
        long executeTime = entry.getHeader().getExecuteTime();
        long delayTime = new Date().getTime() - executeTime;

        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                CanalEntry.TransactionBegin begin = null;
                try {
                    begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                // 打印事务头信息，执行的线程id，事务耗时
                System.out.println("============================================================================> TRANSACTIONBEGIN EVENT :");
                System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                System.out.println("delay time:"+String.valueOf(delayTime));
                System.out.println("BEGIN ----> Thread id:"+begin.getThreadId());
            } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                CanalEntry.TransactionEnd end = null;
                try {
                    end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                // 打印事务提交信息，事务id
                System.out.println("============================================================================> TRANSACTIONEND EVENT :");
                System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                System.out.println("delay time:"+String.valueOf(delayTime));
                System.out.println("END ----> transaction id:"+end.getTransactionId());
            }

        }

        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            //print the row information
            System.out.println("============================================================================> ROWDATA EVENT :");
            System.out.println("binlog name:"+entry.getHeader().getLogfileName());
            System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
            System.out.println("schema name:"+entry.getHeader().getSchemaName());
            System.out.println("table name:"+entry.getHeader().getTableName());
            System.out.println("event type:"+eventType);
            System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
            System.out.println("delay time:"+String.valueOf(delayTime));

            if (rowChage.getIsDdl()) {
                System.out.println("SQL ----> " + rowChage.getSql());
                //continue;
            }

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {//update
                    //before
                    printColumn(rowData.getBeforeColumnsList());
                    //after
                    printColumn(rowData.getAfterColumnsList());
                    //updated
                    printColumn(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList());
                }
            }
        }
    }


    public static void printColumn(List<CanalEntry.Column> columns) {
        //print the column information
        System.out.println("------------------------------------------->column info :");
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            System.out.println(builder);
        }
    }

    public static void printColumn(List<CanalEntry.Column> columns1,List<CanalEntry.Column> columns2) {
        //print the column information
        System.out.println("------------------------------------------->updated column info :");
        for(int i=0;i<=columns2.size()-1;i++){
            StringBuilder builder = new StringBuilder();
            if(columns2.get(i).getIsKey()||columns2.get(i).getUpdated()){
                builder.append(columns2.get(i).getName() + " : " + columns2.get(i).getValue());
                builder.append("    type=" + columns2.get(i).getMysqlType());
                System.out.println(builder);
            }
        }
    }


}
