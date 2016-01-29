package com.github.hackerwin7.mysql.parser.parser.utils;

import com.github.hackerwin7.mysql.parser.protocol.protobuf.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Date;
import java.util.List;

/**
 * Created by hp on 14-9-17.
 */
public class EntryPrinter {

    private CanalEntry.Entry entry;

    public EntryPrinter(CanalEntry.Entry entry) {
        this.entry = entry;
    }

    public static String printEntry(CanalEntry.Entry entry){
        String result = null;
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
                result = "{" +
                        "\"binlog name\":" + entry.getHeader().getLogfileName() + "," +
                        "\"log file offset\":" + String.valueOf(entry.getHeader().getLogfileOffset()) + "," +
                        "\"execute time\":" + String.valueOf(entry.getHeader().getExecuteTime()) + "," +
                        "\"delay time\":" + String.valueOf(delayTime) + "," +
                        "\"BEGIN ----> Thread id\":" + begin.getThreadId();
            } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                CanalEntry.TransactionEnd end = null;
                try {
                    end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                // 打印事务提交信息，事务id
                result = "{" +
                        "\"binlog name\":" + entry.getHeader().getLogfileName() + "," +
                        "\"log file offset\":" + String.valueOf(entry.getHeader().getLogfileOffset()) + "," +
                        "\"execute time\":" + String.valueOf(entry.getHeader().getExecuteTime()) + "," +
                        "\"delay time\":" + String.valueOf(delayTime) + "," +
                        "\"END ----> Thread id\":" + end.getTransactionId();
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
            result = "{" +
                    "\"binlog name\":" + entry.getHeader().getLogfileName() + "," +
                    "\"log file offset\":" + String.valueOf(entry.getHeader().getLogfileOffset()) + "," +
                    "\"schema name\":" + entry.getHeader().getSchemaName() + "," +
                    "\"table name\":" + entry.getHeader().getTableName() + "," +
                    "\"event type\":" + eventType + "," +
                    "\"execute time\":" + String.valueOf(entry.getHeader().getExecuteTime()) + "," +
                    "\"delay time\":" + String.valueOf(delayTime);
            if (rowChage.getIsDdl()) {
                result += "," + "\"SQL:\"" + rowChage.getSql();
            }

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    result += printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    result += printColumn(rowData.getAfterColumnsList());
                } else {//update
                    //before
                    result += printColumn(rowData.getBeforeColumnsList());
                    //after
                    result += printColumn(rowData.getAfterColumnsList());
                    //updated
                    result += printColumn(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList());
                }
            }
        }
        return(result);
    }


    private static String printColumn(List<CanalEntry.Column> columns) {
        //print the column information
        String result = "[";
        int cases = 0;
        for (CanalEntry.Column column : columns) {
            result += "\"" + column.getName() + "\":" + column.getValue() +
                    "," + "\"type=\":" + column.getMysqlType();
            if (column.getUpdated()) {
                result += "," + "\"update\":" + column.getUpdated();
            }
            if(cases != columns.size() -1) {
                result += "|";
            }
            cases ++;
        }
        result += "]";
        return (result);
    }

    private static String printColumn(List<CanalEntry.Column> columns1,List<CanalEntry.Column> columns2) {
        //print the column information
        String result = "[";
        for(int i=0;i<=columns2.size()-1;i++){
            StringBuilder builder = new StringBuilder();
            if(columns2.get(i).getIsKey()||columns2.get(i).getUpdated()){
                builder.append(columns2.get(i).getName() + " : " + columns2.get(i).getValue());
                builder.append("    type=" + columns2.get(i).getMysqlType());
                result += "\"" + columns2.get(i).getName() + "\":" + columns2.get(i).getValue() +
                        "," + "\"type\":" + columns2.get(i).getMysqlType();
            }
            if(i != columns2.size() - 1) {
                result += "|";
            }
        }
        result += "]";
        return(result);
    }

}
