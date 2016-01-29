package com.github.hackerwin7.mysql.parser.parser;

import com.github.hackerwin7.mysql.parser.hbase.driver.HBaseOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.hackerwin7.mysql.parser.parser.utils.EntryPrinter;
import com.github.hackerwin7.mysql.parser.protocol.protobuf.CanalEntry;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-9-17.
 */
public class MysqlParser {

    //parser's logger
    private Logger logger = LoggerFactory.getLogger(MysqlParser.class);

    //hbase operator
    private HBaseOperator hBaseOP;

    //multiple thread queue
    private BlockingQueue<byte[]> bytesQueue;

    // batch size threshold for per fetch the number of the event,if event size >= batchsize then
    // bigFetch() return
    // now by the py test we set the var is 1000

    private int batchsize = 3000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second
    private double secondsize = 1.5;

    //per seconds write the position
    private int secondPer = 60;

    //Global variables
    private byte[] globalReadPos = null;
    private byte[] globalWritePos = null;

    //control variables
    private boolean running;
    private long startTime;

    //constructor
    public MysqlParser() {

        hBaseOP = new HBaseOperator();
        bytesQueue = new LinkedBlockingQueue<byte[]>();

    }

    //prepare the parser configs
    private void preParser() throws IOException{
        running = true;
        startTime = new Date().getTime();
        globalWritePos = null;
        globalReadPos =null;
        findStartPos();
    }

    //find the start position to the global read and global write
    private void findStartPos() throws IOException{
        if(!findStartPosHBase()){
            findStartPosDefault();
        }
    }

    //find the start position according to HBase checkpoint table
    private boolean findStartPosHBase() throws IOException{
        Get get = new Get(Bytes.toBytes(hBaseOP.parserRowKey));
        get.addFamily(hBaseOP.getFamily());
        Result result = hBaseOP.getHBaseData(get, hBaseOP.getCheckpointSchemaName());
        byte[] readPos = result.getValue(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol));
        if(readPos != null) {
            String readPosString = Bytes.toString(readPos);
            Long readPosLong = Long.valueOf(readPosString);
            globalReadPos = Bytes.toBytes(readPosLong);
        }
        byte[] writePos = result.getValue(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol));
        if(writePos != null) {
            String writePosString = Bytes.toString(writePos);
            Long writePosLong = Long.valueOf(writePosString);
            globalWritePos = Bytes.toBytes(writePosLong);
        }
        if(globalReadPos == null || globalWritePos == null){
            return(false);
        }else {
            return (true);
        }
    }

    //find the start position by the default value
    private void findStartPosDefault(){
        if(globalReadPos == null) globalReadPos = Bytes.toBytes(0L);
        if(globalWritePos == null) globalWritePos = Bytes.toBytes(0L);
    }

    //Long switch to bytes specially
    private byte[] LongToStringToBytes(Long value){
        String strVal = String.valueOf(value);
        return(Bytes.toBytes(strVal));
    }
    private Long BytesToStringToLong(byte[] value){
        String strVal = new String(value);
        return(Long.valueOf(strVal));
    }

    //running the process, open the multiple thread to start
    private void runParser() throws IOException{
        //build and start the fetch thread
        FetchThread fetchThread = new FetchThread();
        fetchThread.start();
        //build and start the minute thread
        MinuteTimer minuteThread = new MinuteTimer();
        Timer timer = new Timer();
        timer.schedule(minuteThread, 3 * 1000, secondPer * 1000);
        //build and start the persistence thread
        PersistenceThread persThread = new PersistenceThread();
        persThread.start();
        while(running){
            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){
                logger.error("main thread failed!!!");
                e.printStackTrace();
            }
        }
    }

    //fetch thread
    class FetchThread extends Thread {

        //thread logger
        private Logger logger = LoggerFactory.getLogger(FetchThread.class);

        private boolean fetchable = true;

        private int turnCount = 999;//per turn 100 data

        public void run() {
            while(fetchable){
                //while + sleep
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("sleep error!!!");
                    e.printStackTrace();
                }
                if(isFetchable()) {
                    ResultScanner results = null;
                    Scan scan = new Scan();
                    scan.setBatch(1500);
                    scan.setStartRow(globalReadPos);
                    scan.setStopRow(Bytes.toBytes(Bytes.toLong(globalReadPos) + turnCount));
                    try {
                        results = hBaseOP.getHBaseData(scan, hBaseOP.getEventBytesSchemaName());
                    } catch (IOException e) {
                        logger.error("fetch data failed!!!");
                        e.printStackTrace();
                    }
                    if (results != null) {
                        for (Result result : results) {
                            if (result == null) {//the null is this is the end of batched data
                                break;
                            }
                            byte[] receiveBytes = result.getValue(hBaseOP.getFamily(),
                                    Bytes.toBytes(hBaseOP.eventBytesCol));
                            if (receiveBytes != null) {
                                try {
                                    bytesQueue.put(receiveBytes);
                                } catch (InterruptedException e) {
                                    logger.error("queue put failed!!!");
                                    e.printStackTrace();
                                }
                                globalReadPos = Bytes.toBytes(Bytes.toLong(globalReadPos) + 1L);
                            } else { //the null is this is the end of batched data
                                break;
                            }
                        }
                        //persistence the global read pos
                        Put put = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
                        Long readPosLong = Bytes.toLong(globalReadPos);
                        String readPosString = String.valueOf(readPosLong);
                        put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol), Bytes.toBytes(readPosString));
                        try {
                            hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
                        } catch (IOException e) {
                            logger.error("write global read pos failed!!!");
                            e.printStackTrace();
                        }
                    }
                }
            }
            running = false;//close all running process
        }

        //monitor the hbase globalReadPos whether have inserted data
        private boolean isFetchable(){
            //monitor the hbase globalReadPos whether have the data inserted
            Get get = new Get(globalReadPos);
            get.addColumn(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventBytesCol));
            Result result = null;
            try {
                result = hBaseOP.getHBaseData(get, hBaseOP.getEventBytesSchemaName());
            } catch (IOException e){
                logger.error("fetch single data failed!!!");
                e.printStackTrace();
            }
            if(result == null) return false;
            byte[] receiveBytes = result.getValue(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventBytesCol));
            if(receiveBytes != null) return true;
            else return false;
        }
    }

    //per minute run the function to record the read pos and write pos to checkpoint in HBase
    class MinuteTimer extends TimerTask {

        //logger
        private Logger logger = LoggerFactory.getLogger(MinuteTimer.class);

        public void run() {
            if(globalReadPos != null && globalWritePos != null) {
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String time = sdf.format(cal.getTime());
                String rowKey = hBaseOP.parserRowKey + ":" + time;
                Put put = new Put(Bytes.toBytes(rowKey));
                Long readPosLong = Bytes.toLong(globalReadPos);
                String readPosString = String.valueOf(readPosLong);
                Long writePosLong = Bytes.toLong(globalWritePos);
                String writePosString = String.valueOf(writePosLong);
                put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol), Bytes.toBytes(readPosString));
                put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(writePosString));
                try {
                    hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
                }catch (IOException e){
                    logger.error("minute persistence read pos and write pos failed!!!");
                    e.printStackTrace();
                }
            }
        }
    }

    //persistence batch data and pos thread
    class PersistenceThread extends Thread {

        //logger
        private Logger logger = LoggerFactory.getLogger(PersistenceThread.class);

        //BytesList get data from BytesQueue
        private List<byte[]> bytesList = new ArrayList<byte[]>();

        //control
        private boolean persistenceRunning = true;

        public void run() {
            startTime = new Date().getTime();
            bytesList.clear();
            while(persistenceRunning){
                //while + sleep
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    logger.error("sleep error!!!");
                    e.printStackTrace();
                }
                while(!bytesQueue.isEmpty()) {
                    try {
                        byte[] receiveBytes = bytesQueue.take();
                        bytesList.add(receiveBytes);
                        //per turn do not load much data
                        if(bytesList.size() >= batchsize) break;
                    } catch (InterruptedException e) {
                        logger.error("take data from queue failed!!!");
                        e.printStackTrace();
                    }
                }
                //persistence the batched size entry string  to entry table in HBase and
                // write pos to checkpoint
                if(bytesList.size() >= batchsize ||
                        new Date().getTime() - startTime > secondsize * 1000) {
                    if(bytesList.size() > 0) {
                        try {
                            //persistence entry data
                            persistenceEntry();
                        } catch (IOException e) {
                            logger.error("persistence entry data failed!!!");
                            e.printStackTrace();
                        }
                        try {
                            //persistence pos data
                            persistencePos();
                        } catch (IOException e) {
                            logger.error("persistence write pos failed!!!");
                            e.printStackTrace();
                        }
                        //clear list
                        bytesList.clear();
                        startTime = new Date().getTime();
                    }
                }
            }
        }

        //persistence entry data
        private void persistenceEntry() throws IOException{
            List<Put> puts = new ArrayList<Put>();
            for(byte[] bytes : bytesList) {
                CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(bytes);
                System.out.println("--------------------------->get entry : " +
                        entry.getEntryType() +
                ",-----> now pos : " +
                        entry.getHeader().getLogfileOffset() +
                ",-----> next pos : " +
                        (entry.getHeader().getLogfileOffset() + entry.getHeader().getEventLength()) +
                ",-----> binlog file : " +
                        entry.getHeader().getLogfileName() +
                ",-----> schema name : " +
                        entry.getHeader().getSchemaName() +
                ",-----> table name : " +
                        entry.getHeader().getTableName()
                );
                String entryString = EntryToString(entry);
                Put put = new Put(globalWritePos);
                put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(entryString));
                puts.add(put);
                globalWritePos = Bytes.toBytes(Bytes.toLong(globalWritePos) + 1L);
            }
            if(puts.size() > 0) hBaseOP.putHBaseData(puts, hBaseOP.getEntryDataSchemaName());
        }

        //Entry to String
        private String EntryToString(CanalEntry.Entry entry) {
            return(EntryPrinter.printEntry(entry));
        }

        //persistence write pos data
        private void persistencePos() throws IOException {
            if(bytesList.size() > 0) {
                Put put = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
                Long writePosLong = Bytes.toLong(globalWritePos);
                String writePosString = String.valueOf(writePosLong);
                put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(writePosString));
                hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
            }
        }
    }

    //after parser
    private void afterParser(){

    }

    //main process
    public void mainProc() throws IOException{
        preParser();
        runParser();
        afterParser();
    }
}
