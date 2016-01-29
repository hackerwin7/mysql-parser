package com.github.hackerwin7.mysql.parser.parser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.bdp.magpie.MagpieExecutor;
import com.github.hackerwin7.mysql.parser.hbase.driver.HBaseOperator;
import com.github.hackerwin7.mysql.parser.hbase.utils.HData;
import com.github.hackerwin7.mysql.parser.monitor.ParserMonitor;
import net.sf.json.JSONObject;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.hackerwin7.mysql.parser.parser.utils.ParserConfig;
import com.github.hackerwin7.mysql.parser.protocol.avro.EventEntryAvro;
import com.github.hackerwin7.mysql.parser.protocol.json.ConfigJson;
import com.github.hackerwin7.mysql.parser.protocol.protobuf.CanalEntry;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-9-22.
 */
public class HandlerForMagpieHBase implements MagpieExecutor {

    //parser's logger
    private Logger logger = LoggerFactory.getLogger(HandlerForMagpieHBase.class);

    //configuration
    private ParserConfig configer;

    //hbase operator
    private HBaseOperator hBaseOP;

    //final queue max size
    private final int MAXQUEUE = 30000;

    //multiple thread queue
    private BlockingQueue<HData> rowQueue;

    // batch size threshold for per fetch the number of the event,if event size >= batchsize then
    // bigFetch() return
    // now by the py test we set the var is 1000

    private int batchsize = 100000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second
    private double secondsize = 1.0;

    //per seconds write the position
    private int secondPer = 60;

    //Global variables
    private byte[] globalReadPos = null;
    private byte[] globalWritePos = null;

    //control variables
    private boolean running;
    private long startTime;
    private List<HData> rowList;

    //monitor
    private ParserMonitor monitor;

    //thread
    FetchThread fetchThread;
    MinuteTimer minuteThread;
    Timer timer;

    //constructor
    public HandlerForMagpieHBase(ParserConfig cnf) {

        this.configer = cnf;
    }

    public HandlerForMagpieHBase(File file) throws IOException{
        if(file.exists()) {
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            Properties pro = new Properties();
            pro.load(in);
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

        //adjust the config
        ConfigJson configJson = new ConfigJson(id);
        JSONObject jRoot = configJson.getJson();
        if(jRoot != null) {
            JSONObject jContent = jRoot.getJSONObject("info").getJSONObject("content");
            configer.setHbaseRootDir(jContent.getString("HbaseRootDir"));
            configer.setHbaseDistributed(jContent.getString("HbaseDistributed"));
            configer.setHbaseZkQuorum(jContent.getString("HbaseZkQuorum"));
            configer.setHbaseZkPort(jContent.getString("HbaseZkPort"));
            configer.setDfsSocketTimeout(jContent.getString("DfsSocketTimeout"));
        }

        //initialize hbase
        hBaseOP = new HBaseOperator(id);
        hBaseOP.getConf().set("hbase.rootdir",configer.getHbaseRootDir());
        hBaseOP.getConf().set("hbase.cluster.distributed",configer.getHbaseDistributed());
        hBaseOP.getConf().set("hbase.zookeeper.quorum",configer.getHbaseZkQuorum());
        hBaseOP.getConf().set("hbase.zookeeper.property.clientPort",configer.getHbaseZkPort());
        hBaseOP.getConf().set("dfs.socket.timeout", configer.getDfsSocketTimeout());
        hBaseOP.connect();
        rowQueue = new LinkedBlockingQueue<HData>(MAXQUEUE);

        //initialize variables
        running = true;
        startTime = new Date().getTime();
        globalWritePos = null;
        globalReadPos =null;
        findStartPos();
        logger.info("global read pos is " + Bytes.toLong(globalReadPos));

        //run parser thread
        //build and start the fetch thread
        fetchThread = new FetchThread();
        fetchThread.start();
        //build and start the minute thread
        minuteThread = new MinuteTimer();
        timer = new Timer();
        timer.schedule(minuteThread, 3 * 1000, secondPer * 1000);

        //persistence variable initialize
        startTime = new Date().getTime();
        rowList = new ArrayList<HData>();

        //monitor
        monitor = new ParserMonitor();

        //log info
        logger.info("start the mysql-parser successfully...");
    }

    //find the start position to the global read and global write
    private void findStartPos() throws IOException {
        logger.info("find start position for parser...");
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

    //fetch thread
    class FetchThread extends Thread {

        //thread logger
        private Logger logger = LoggerFactory.getLogger(FetchThread.class);

        private boolean fetchable = true;

        private int turnCount = 10000;//per turn 100000 data

        private int maxOneRow = 5000;//set batch

        private HTable hIsFetch;

        private ParserMonitor monitor = new ParserMonitor();

        public void setFetchable(boolean is) {
            fetchable = is;
        }

        public void run() {
            try {
                hIsFetch = new HTable(hBaseOP.getConf(), hBaseOP.getEventBytesSchemaName());
            } catch (IOException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
            while(fetchable){
                if(isFetchable()) {
                    monitor.fetchStart = System.currentTimeMillis();
                    ResultScanner results = null;
                    Scan scan = new Scan();
                    scan.setStartRow(globalReadPos);
                    scan.setStopRow(Bytes.toBytes(Bytes.toLong(globalReadPos) + turnCount));
                    scan.addColumn(hBaseOP.getFamily(),
                            Bytes.toBytes(hBaseOP.eventBytesCol));
                    scan.setCaching(turnCount);
                    scan.setCacheBlocks(true);
                    try {
                        results = hBaseOP.getHBaseData(scan, hBaseOP.getEventBytesSchemaName());
                    } catch (IOException e) {
                        logger.error("fetch data failed!!!");
                        e.printStackTrace();
                    }
                    if (results != null) {
                        int resultsSize = 0;
                        for (Result result : results) {
                            //this rowdata and next rowkey save into hData
                            globalReadPos = Bytes.toBytes(Bytes.toLong(globalReadPos) + 1L);
                            if (result == null) {//the null is this is the end of batched data
                                break;
                            }
                            resultsSize++;
                            byte[] receiveBytes = result.getValue(hBaseOP.getFamily(),
                                    Bytes.toBytes(hBaseOP.eventBytesCol));
                            monitor.batchSize += receiveBytes.length;
                            byte[] receiveRowKey = globalReadPos;
                            HData hData = new HData(receiveRowKey,receiveBytes);
                            if (receiveBytes != null) {
                                try {
                                    rowQueue.put(hData);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            } else { //the null is this is the end of batched data
                                break;
                            }
                        }
                        monitor.fetchNum += resultsSize;
                        //it's a big bug!!!
                        //if we fetched data and persistence the position bug we failed to persistence the data
                        //to hbase entry table then we will lost these data
                        // ,persistence the global read pos
                        /*Put put = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
                        Long readPosLong = Bytes.toLong(globalReadPos);
                        String readPosString = String.valueOf(readPosLong);
                        put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol), Bytes.toBytes(readPosString));
                        try {
                            hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
                        } catch (IOException e) {
                            logger.error("write global read pos failed!!!");
                            e.printStackTrace();
                        }*/
                    }
                    if(results != null) results.close();
                    monitor.fetchEnd = System.currentTimeMillis();
                    logger.info("======> fetch thread : ");
                    logger.info("---> fetch thread during sum time : " + (monitor.fetchEnd - monitor.fetchStart));
                    logger.info("---> fetch the events (bytes) num : " + monitor.fetchNum);
                    logger.info("---> fetch the events size : " + monitor.batchSize);
                    monitor.clear();
                }
            }
            running = false;//close all running process
            try {
                hIsFetch.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }

        //monitor the hbase globalReadPos whether have inserted data
        private boolean isFetchable(){
            //monitor the hbase globalReadPos whether have the data inserted
            Get get = new Get(globalReadPos);
            get.addColumn(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventBytesCol));
            boolean result = false;
            try {
                result  = hIsFetch.exists(get);
            } catch (IOException e){
                logger.error("fetch single data failed!!!");
                e.printStackTrace();
            }
            return result;
        }
    }

    //per minute run the function to record the read pos and write pos to checkpoint in HBase
    class MinuteTimer extends TimerTask {

        //logger
        private Logger logger = LoggerFactory.getLogger(MinuteTimer.class);

        public void run() {
            if(globalReadPos != null && globalWritePos != null) {
                //logger.info("per minute persistence the position into HBase...");
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String time = sdf.format(cal.getTime());
                String rowKey = hBaseOP.parserRowKey + "##" + time;
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
                logger.info("======> per minute persistence the position into HBase...");
                logger.info("---> row key is :" + rowKey + "," +
                        "col is :" + readPosString + "," +
                        "col is :" + writePosString);
            }
        }
    }

    public void reload(String id) {
    }


    public void pause(String id) throws Exception {
    }


    public void close(String id) throws Exception {
        fetchThread.setFetchable(false);//stop the fetch thread
        minuteThread.cancel();
        timer.cancel();
        hBaseOP.disconnect();
    }


    public void run() throws Exception {
        delay(1);
        while(!rowQueue.isEmpty()) {
            try {
                HData hData = rowQueue.take();
                rowList.add(hData);
                //per turn do not load much data
                if(rowList.size() >= batchsize) break;
            } catch (InterruptedException e) {
                logger.error("take data from queue failed!!!");
                e.printStackTrace();
            }
        }
        //persistence the batched size entry string  to entry table in HBase and
        // write pos to checkpoint
        if(rowList.size() >= batchsize ||
                new Date().getTime() - startTime > secondsize * 1000) {
            if(rowList.size() > 0) {
                monitor.persisNum = rowList.size();
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
                rowList.clear();
                startTime = new Date().getTime();
            }
        }
        if(monitor.persisNum > 0) {
            logger.info("---> persistence deal during time : " + (monitor.persistenceEnd - monitor.persistenceStart));
            logger.info("---> write hbase during time : " + (monitor.hbaseWriteEnd - monitor.hbaseWriteStart));
            logger.info("---> entry list to bytes (avro) sum size is " + monitor.batchSize);
            logger.info("---> the number if entry list is " + monitor.persisNum);
            monitor.clear();
        }
    }

    //persistence entry data
    private void persistenceEntry() throws IOException{
        List<Put> puts = new ArrayList<Put>();
        int i = 0;
        CanalEntry.Entry lastEntry = null;
        String colValue = "";
        monitor.persistenceStart = System.currentTimeMillis();
        for(HData hData : rowList) {
            CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(hData.rowData);
            lastEntry = entry;
            if(entry != null && entry.getEntryType() == CanalEntry.EntryType.ROWDATA) colValue = getEntryCol(entry);
            //log monitor
            //logInfoEntry(entry);
//            String entryString = entryToString(entry);
//            Put put = new Put(globalWritePos);
//            put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(entryString));
            Put put = new Put(globalWritePos);
            byte[] avroBytes = getBytesFromEntryToAvro(entry);
            monitor.batchSize += avroBytes.length;
            put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), avroBytes);
            puts.add(put);
            globalWritePos = Bytes.toBytes(Bytes.toLong(globalWritePos) + 1L);
        }
        monitor.persistenceEnd = System.currentTimeMillis();
        if(lastEntry != null) {
            if(rowList.size() > 0) logger.info("======> persistence the " + rowList.size() + " entries "
                    + " the batched last column is " + colValue);
            logInfoEntry(lastEntry);
        }
        //persistence a batch row data set
        if(puts.size() > 0) {
            monitor.hbaseWriteStart = System.currentTimeMillis();
            hBaseOP.putHBaseData(puts, hBaseOP.getEntryDataSchemaName());
            monitor.hbaseWriteEnd = System.currentTimeMillis();
        }
    }


    private String getEntryType(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String operationType = "";
            switch (rowChange.getEventType()) {
                case INSERT:
                    return "INSERT";
                case UPDATE:
                    return "UPDATE";
                case DELETE:
                    return "DELETE";
                case CREATE:
                    return "CREATE";
                case ALTER:
                    return "ALTER";
                case ERASE:
                    return "ERASE";
                case QUERY:
                    return "QUERY";
                case TRUNCATE:
                    return "TRUNCATE";
                case RENAME:
                    return "RENAME";
                case CINDEX:
                    return "CINDEX";
                case DINDEX:
                    return "DINDEX";
                default:
                    return "UNKNOWN";
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return "NULL";
    }

    private EventEntryAvro entryToAvro(CanalEntry.Entry entry) {
        EventEntryAvro entryAvro = new EventEntryAvro();
        entryAvro.setDb(entry.getHeader().getSchemaName());
        entryAvro.setSch(entry.getHeader().getSchemaName());
        entryAvro.setTab(entry.getHeader().getTableName());
        entryAvro.setOpt(getEntryType(entry));
        entryAvro.setTs(new Date().getTime());
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            if(rowChange.getIsDdl()) entryAvro.setDdl(rowChange.getSql());
            else entryAvro.setDdl("");
            entryAvro.setErr("");
            //current and source
            for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if(rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                    List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    for(CanalEntry.Column column : columns) {
                        sourceCols.put(column.getName(),column.getValue());
                        if(column.getIsKey()) {
                            currentCols.put(column.getName(),column.getValue());
                        }
                    }
                    entryAvro.setCur(currentCols);
                    entryAvro.setSrc(sourceCols);
                } else if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                    List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    for(CanalEntry.Column column : columns) {
                        currentCols.put(column.getName(),column.getValue());
                    }
                    entryAvro.setSrc(sourceCols);
                    entryAvro.setCur(currentCols);
                } else if(rowChange.getEventType() == CanalEntry.EventType.UPDATE) {
                    List<CanalEntry.Column> columnsSource = rowData.getBeforeColumnsList();
                    List<CanalEntry.Column> columnsCurrent = rowData.getAfterColumnsList();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    for(int i=0,j=0;i<=columnsCurrent.size()-1 || j<=columnsSource.size()-1;i++,j++) {
                        if(i<=columnsCurrent.size()-1)
                            currentCols.put(columnsCurrent.get(i).getName(),columnsCurrent.get(i).getValue());
                        if(j<=columnsSource.size()-1)
                            sourceCols.put(columnsSource.get(j).getName(),columnsSource.get(j).getValue());
                    }
                } else {
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    entryAvro.setCur(currentCols);
                    entryAvro.setSrc(sourceCols);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return(entryAvro);
    }

    private byte[] getBytesFromAvro(EventEntryAvro avro) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        DatumWriter<EventEntryAvro> writer = new SpecificDatumWriter<EventEntryAvro>(EventEntryAvro.getClassSchema());
        try {
            writer.write(avro,encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] value = out.toByteArray();
        return value;
    }

    private EventEntryAvro getAvroFromBytes(byte[] value) {
        SpecificDatumReader <EventEntryAvro> reader = new SpecificDatumReader<EventEntryAvro>(EventEntryAvro.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value,null);
        EventEntryAvro avro = null;
        try {
            avro = reader.read(null,decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avro;
    }

    private byte[] getBytesFromEntryToAvro(CanalEntry.Entry entry) {
        return getBytesFromAvro(entryToAvro(entry));
    }

    //persistence write pos data
    private void persistencePos() throws IOException {
        if(rowList.size() > 0) {
            //persistence the parser read pos
            Put readPut = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
            Long readPosLong = Bytes.toLong(rowList.get(rowList.size()-1).rowKey);
            String readPosString = String.valueOf(readPosLong);
            readPut.add(hBaseOP.getFamily(),Bytes.toBytes(hBaseOP.eventRowCol),Bytes.toBytes(readPosString));
            hBaseOP.putHBaseData(readPut,hBaseOP.getCheckpointSchemaName());
            //persistence the parser write pos
            Put put = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
            Long writePosLong = Bytes.toLong(globalWritePos);
            String writePosString = String.valueOf(writePosLong);
            put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(writePosString));
            hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
        }
    }


    private void logInfoEntry(CanalEntry.Entry lastEntry) {
        if(lastEntry != null) {
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(lastEntry.getStoreValue());
                String colValue = "null";
                if (rowChange.getRowDatasList().size() > 0) {
                    CanalEntry.RowData rowData = rowChange.getRowDatas(0);
                    if (rowData.getAfterColumnsList().size() > 0) {
                        colValue = rowData.getAfterColumns(0).getName() + " ## " + rowData.getAfterColumns(0).getValue();
                    }
                }
                logger.info("--->get entry : " +
                                lastEntry.getEntryType() +
                                ", now pos : " +
                                lastEntry.getHeader().getLogfileOffset() +
                                ", next pos : " +
                                (lastEntry.getHeader().getLogfileOffset() + lastEntry.getHeader().getEventLength()) +
                                ", binlog file : " +
                                lastEntry.getHeader().getLogfileName() +
                                ", schema name : " +
                                lastEntry.getHeader().getSchemaName() +
                                ", table name : " +
                                lastEntry.getHeader().getTableName() +
                                ", column info : " +
                                colValue +
                                ", type : " +
                                getEntryType(lastEntry)
                );
            } catch (InvalidProtocolBufferException e) {
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

    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

}
