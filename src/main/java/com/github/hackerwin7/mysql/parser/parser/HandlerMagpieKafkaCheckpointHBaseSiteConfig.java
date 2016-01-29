package com.github.hackerwin7.mysql.parser.parser;

import com.github.hackerwin7.mysql.parser.kafka.driver.producer.KafkaSender;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaMetaMsg;
import com.github.hackerwin7.mysql.parser.monitor.JrdwMonitorVo;
import com.github.hackerwin7.mysql.parser.monitor.ParserMonitor;
import com.github.hackerwin7.mysql.parser.monitor.constants.JDMysqlParserMonitorType;
import com.github.hackerwin7.mysql.parser.parser.utils.KafkaPosition;
import com.github.hackerwin7.mysql.parser.parser.utils.ParserConf;
import com.github.hackerwin7.mysql.parser.protocol.avro.EventEntryAvro;
import com.github.hackerwin7.mysql.parser.protocol.json.JSONConvert;
import com.github.hackerwin7.mysql.parser.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.parser.zk.client.ZkExecutor;
import com.github.hackerwin7.mysql.parser.zk.utils.ZkConf;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.bdp.magpie.MagpieExecutor;
import com.jd.bdp.monitors.commons.util.CheckpointUtil;
import com.jd.bdp.monitors.constants.ToKafkaMonitorType;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-12-15.
 */
public class HandlerMagpieKafkaCheckpointHBaseSiteConfig implements MagpieExecutor {
    //logger
    private Logger logger = LoggerFactory.getLogger(HandlerMagpieKafkaCheckpointHBaseSiteConfig.class);
    //constants
    public static final String LOG_HEAD_LINE = "===================================================> ";
    //final static var
    private static final long LONGMAX = 9223372036854775807L;
    //config
    private ParserConf config = new ParserConf();
    //job id
    private String jobId;
    //kafka
    private KafkaSender msgSender;
    //phoenix kafka sender
    private KafkaSender phMonitorSender;
    //zk
    private ZkExecutor zk;
    //blocking queue
    private BlockingQueue<KafkaMetaMsg> msgQueue;
    //batch id and in batch id
    private long batchId = 0;
    private long inId = 0;
    private long uId = 0;//unique parser id
    private long confirmOffset = 0;//globalvar is for minute persistence, confirmOffset is for persistence
    //thread communicate
    private int globalFetchThread = 0;
    //global var
    private long globalOffset = -1;
    private long globalBatchId = -1;
    private long globalInBatchId = -1;
    private long globalUId = -1;
    //global start time
    private long startTime;
    //thread
    private Fetcher fetcher;
    private Timer timer;
    private ConfirmCP confirm;
    private Timer htimer;
    private HeartBeat hb;
    //monitor
    private ParserMonitor monitor;
    //global var
    List<CanalEntry.Entry> entryList;
    CanalEntry.Entry last = null;
    CanalEntry.Entry pre = null;
    List<KeyedMessage<String, byte[]>> messageList;
    //thread survival confirmation
    private boolean fetchSurvival = true;
    //thread finish load or run one times
    private boolean isFetchRunning = false;
    //checkpoint util
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

    private void delayMin(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void init() throws Exception {
        //logger
        logger.info("initializing......");
        //id
        config.jobId = jobId;
        //load environment config (local,off-line,on-line)
        config.initConfigFromSiteService();
        //jobId
        jobId = config.jobId;
        //mid
        uId = config.mid;
        //kafka
        KafkaConf kcnf = new KafkaConf();
        kcnf.brokerList = config.brokerList;
        kcnf.port = config.kafkaPort;
        kcnf.topic = config.topic;
        kcnf.acks = config.acks;
        kcnf.compression = config.kafkaCompression;
        msgSender = new KafkaSender(kcnf);
        msgSender.connect();
        //phoenix monitor sender
        KafkaConf kpcnf = new KafkaConf();
        kpcnf.brokerList = config.phKaBrokerList;
        kpcnf.port = config.phKaPort;
        kpcnf.topic = config.phKaTopic;
        kpcnf.acks = config.phKaAcks;
        phMonitorSender = new KafkaSender(kpcnf);
        phMonitorSender.connect();
        //zk
        ZkConf zcnf = new ZkConf();
        zcnf.zkServers = config.zkServers;
        zk = new ZkExecutor(zcnf);
        boolean isZk = false;
        int retryZk = 0;
        while (!isZk) {
            if(retryZk >= config.retrys) {
                globalFetchThread = 1;
                throw new RetryTimesOutException("reload job......");
            }
            retryZk++;
            try {
                zk.connect();
                isZk = true;
            } catch (Exception e) {
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ParserMonitor exMonitor = new ParserMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                sendMonitor.start();
                logger.error("connect zk failed , retrying......");
                e.printStackTrace();
                delay(3);
            }
        }
        //initZk();
        //queue
        msgQueue = new LinkedBlockingQueue<KafkaMetaMsg>(config.queuesize);
        //batchid inId
        batchId = 0;
        inId = 0;
        confirmOffset = LONGMAX;
        //global thread
        globalFetchThread = 0;
        //global var
        globalOffset = -1;
        globalBatchId = -1;
        globalInBatchId = -1;
        globalUId = -1;
        //start time
        startTime = System.currentTimeMillis();
        //thread config
        KafkaConf fetchCnf = new KafkaConf();
        fetchCnf.brokerSeeds = config.brokerSeeds;
        fetchCnf.portList = config.portList;
        fetchCnf.topic = config.topic;
        fetchCnf.clientName = config.clientName;
        fetcher = new Fetcher(fetchCnf);
        timer = new Timer();
        confirm = new ConfirmCP();
        htimer = new Timer();
        hb = new HeartBeat();
        //monitor
        monitor = new ParserMonitor();
        //global var
        entryList = new ArrayList<CanalEntry.Entry>();
        messageList = new ArrayList<KeyedMessage<String, byte[]>>();
        isFetchRunning = false;
        cpUtil = new CheckpointUtil();
    }

    private void initZk() throws Exception {
        boolean isZk = false;
        int retryZk = 0;
        while (!isZk) {
            if(retryZk >= config.retrys) {
                globalFetchThread = 1;
                throw new RetryTimesOutException("reload job......");
            }
            retryZk++;
            try {
                if (!zk.exists(config.rootPath)) {
                    zk.create(config.rootPath, "");
                }
                if (!zk.exists(config.persisPath)) {
                    zk.create(config.persisPath, "");
                }
                if (!zk.exists(config.minutePath)) {
                    zk.create(config.minutePath, "");
                }
                isZk = true;
            } catch (Exception e) {
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ParserMonitor exMonitor = new ParserMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, jobId);
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

    public void prepare(String id) throws Exception {
        jobId = id;
        try {
            init();
        } catch (RetryTimesOutException e) {
            logger.error(e.getMessage());
            return;
        }
        //start thread
        fetchSurvival = true;
        fetcher.start();
        timer.schedule(confirm, ParserConf.CONFIRM_DELAY_START, ParserConf.CONFIRM_TIMER_INTERVAL);
        htimer.schedule(hb, ParserConf.HEART_BEAT_DELAY_START, ParserConf.HEART_BEAT_TIMER_INTERVAL);
        //ip monitor
        //send monitor
        final String localIp = InetAddress.getLocalHost().getHostAddress();
        Thread sendMonitor = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ParserMonitor ipMonitor = new ParserMonitor();
                    ipMonitor.ip = localIp;
                    JrdwMonitorVo jmv = ipMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.IP_MONITOR, jobId);
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
        logger.info("start the parser successfully...");
    }

    class Fetcher extends Thread {
        private Logger logger = LoggerFactory.getLogger(Fetcher.class);
        private KafkaConf conf;
        private List<String> replicaBrokers = new ArrayList<String>();
        private List<Integer> replicaPorts = new ArrayList<Integer>();
        public int retry = 3;
        public SimpleConsumer consumer;
        public boolean isFetch = true;
        private ParserMonitor fetchMonitor = new ParserMonitor();
        private int retrys = 0;
        private ParserMonitor minuteMonitor = new ParserMonitor();
        public FetchMonitorMin timerMonitor = new FetchMonitorMin();
        public Timer timer = new Timer();
        private long recordPreOffset = 0;
        private long fetchZkOffset = 0;

        class FetchMonitorMin extends TimerTask {
            private Logger logger = LoggerFactory.getLogger(FetchMonitorMin.class);

            public void run() {
                try {
                    minuteMonitor.lastOffset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), conf.clientName);
                    logger.info("================> per minute monitor");
                    logger.info("---> fetch num:" + minuteMonitor.fetchNum + " entries");
                    logger.info("---> fetch sum size:" + minuteMonitor.batchSize / config.mbUnit + " MB");
                    if(minuteMonitor.nowOffset == 0) {
                        minuteMonitor.nowOffset = recordPreOffset;
                        if(minuteMonitor.nowOffset == 0) {
                            minuteMonitor.nowOffset = fetchZkOffset;
                        }
                        recordPreOffset = minuteMonitor.nowOffset;
                    } else {
                        recordPreOffset = minuteMonitor.nowOffset;
                    }
                    if(minuteMonitor.nowOffset <= 0 || minuteMonitor.nowOffset > minuteMonitor.lastOffset) {
                        //no data then delay num is 0
                        minuteMonitor.nowOffset = minuteMonitor.lastOffset;
                    }
                    logger.info("---> delay num:" + (minuteMonitor.lastOffset - minuteMonitor.nowOffset) + " offsets" + ", last offset:" + minuteMonitor.lastOffset + ", now offset:" + minuteMonitor.nowOffset);
                    //send monitor phoenix
                    JrdwMonitorVo jmv = minuteMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.FETCH_MONITOR, jobId);
                    String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                    phMonitorSender.sendKeyMsg(km);
                    minuteMonitor.clear();
                    logger.info("fetch json monitor :" + jsonStr);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public Fetcher(KafkaConf cnf) {
            conf = cnf;
        }

        public PartitionMetadata findLeader(List<String> brokers, int port, String topic, int partition) throws Exception {
            PartitionMetadata returnData = null;
            SimpleConsumer sconsumer = null;
            loop:
            for (String broker : brokers) {
                try {
                    sconsumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, conf.clientName);
                    List<String> topics = Collections.singletonList(topic);
                    TopicMetadataRequest req = new TopicMetadataRequest(topics);
                    TopicMetadataResponse rep = sconsumer.send(req);
                    List<TopicMetadata> topicMetadatas = rep.topicsMetadata();
                    for (TopicMetadata topicMetadata : topicMetadatas) {
                        for (PartitionMetadata part : topicMetadata.partitionsMetadata()) {
                            if (part.partitionId() == partition) {
                                returnData = part;
                                break loop;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("find leader failed!! , " + e.getMessage(), e);
                } finally {
                    if(sconsumer != null) {
                        sconsumer.close();
                        sconsumer = null;
                    }
                }
            }
            if(returnData != null) {
                replicaBrokers.clear();
                for (Broker broker : returnData.replicas()) {
                    replicaBrokers.add(broker.host());
                }
            }
            return returnData;
        }

        //two List length must be equal
        public PartitionMetadata findLeader(List<String> brokers, List<Integer> ports, String topic, int partition) {
            PartitionMetadata returnData = null;
            SimpleConsumer sconsumer = null;
            loop:
            for (int i = 0; i <= brokers.size() - 1; i++) {
                try {
                    logger.info("scan broker:" + brokers.get(i) + " # " + ports.get(i));
                    sconsumer = new SimpleConsumer(brokers.get(i), ports.get(i), 100000, 64 * 1024, "leader");
                    List<String> topics = Collections.singletonList(topic);
                    TopicMetadataRequest req = new TopicMetadataRequest(topics);
                    TopicMetadataResponse rep = sconsumer.send(req);
                    List<TopicMetadata> topicMetadatas = rep.topicsMetadata();
                    for (TopicMetadata topicMetadata : topicMetadatas) {
                        for (PartitionMetadata part : topicMetadata.partitionsMetadata()) {
                            if (part.partitionId() == partition) {
                                returnData = part;
                                break loop;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("find leader failed!! , " + e.getMessage(), e);
                } finally {
                    if(sconsumer != null) {
                        sconsumer.close();
                        sconsumer = null;
                    }
                }
            }
            if(returnData != null) {
                replicaBrokers.clear();
                replicaPorts.clear();
                for (Broker broker : returnData.replicas()) {
                    logger.info("return broker :" + broker.host() + " # " + broker.port());
                    replicaBrokers.add(broker.host());
                    replicaPorts.add(broker.port());
                }
            }
            return returnData;
        }

        public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whitchTime, String clientName) throws Exception {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whitchTime, 1));
            OffsetRequest req = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
            OffsetResponse rep = consumer.getOffsetsBefore(req);
            if(rep.hasError()) {
                logger.error("Error fetching data Offset Data the Broker. Reason: " + rep.errorCode(topic, partition));
                return -1;
            }
            long[] offsets = rep.offsets(topic, partition);
            return offsets[0];
        }

        public String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
            for(int i = 0; i < retry; i++) {
                boolean goToSleep = false;
                PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
                if(metadata == null) {
                    goToSleep = true;
                } else if (metadata.leader() == null) {
                    goToSleep = true;
                } else if(oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                    goToSleep = true;
                } else {
                    return metadata.leader().host();
                }
                if(goToSleep) {
                    delay(1);
                }
            }
            logger.error("Unable to find new leader after Broker failure. Exiting");
            throw new Exception("Unable to find new leader after Broker failure. Exiting");
        }

        public String findNewLeader(String oldLeader, String topic, int partition) throws Exception {
            for(int i = 0; i < retry; i++) {
                boolean goToSleep = false;
                PartitionMetadata metadata = findLeader(replicaBrokers, replicaPorts, topic, partition);
                if(metadata == null) {
                    goToSleep = true;
                } else if (metadata.leader() == null) {
                    goToSleep = true;
                } else if(oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                    goToSleep = true;
                } else {
                    return metadata.leader().host();
                }
                if(goToSleep) {
                    delay(1);
                }
            }
            logger.error("Unable to find new leader after Broker failure. Exiting");
            throw new Exception("Unable to find new leader after Broker failure. Exiting");
        }

        private KafkaPosition findCheckpointHBase() throws Exception {
            KafkaPosition position = null;
            String value = cpUtil.readCheckpoint(jobId, CheckpointUtil.CURRENT_CHECKPOINT);
            if(value == null || value.equals("")) {
                throw new Exception("fetch checkpoint is null, reload the job " + jobId);
            }
            String vs[] = value.split(":");
            if(vs.length != 6) {
                throw new Exception("fetch checkpoint's format is error, checkpoint = " + value + ", reload the job " + jobId);
            }
            position = new KafkaPosition();
            position.topic = vs[0];
            position.partition = Integer.valueOf(vs[1]);
            position.offset = Long.valueOf(vs[2]);
            position.batchId = Long.valueOf(vs[3]);
            position.inId = Long.valueOf(vs[4]);
            position.uId = Long.valueOf(vs[5]);
            uId = position.uId;//priority: zk position of mid > config of mid
            confirmOffset = position.offset;
            return position;
        }

        private KafkaPosition findPosFromZk() {
            KafkaPosition returnPos = null;
            try {
                String zkPos = config.persisPath + "/" + jobId;
                String getStr = zk.get(zkPos);
                if(getStr == null || getStr.equals("")) {
                    logger.info("null zk position......");
                    returnPos = new KafkaPosition();
                    returnPos.topic = config.topic;
                    returnPos.partition = config.partition;
                    returnPos.offset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), "get_init");
                    returnPos.batchId = 0;
                    returnPos.inId = 0;
                    returnPos.uId = 0;
                    returnPos.offset = LONGMAX;
                    return returnPos;
                }
                String[] ss = getStr.split(":");
                if(ss.length !=6) {
                    logger.info("error zk position......");
                    zk.delete(zkPos);
                    logger.error("zk position format is error...");
                    return null;
                }
                returnPos = new KafkaPosition();
                returnPos.topic = ss[0];
                returnPos.partition = Integer.valueOf(ss[1]);
                returnPos.offset = Long.valueOf(ss[2]);
                returnPos.batchId = Long.valueOf(ss[3]);
                returnPos.inId = Long.valueOf(ss[4]);
                returnPos.uId = Long.valueOf(ss[5]);
                uId = returnPos.uId;//priority: zk position of mid > config of mid
                confirmOffset = returnPos.offset;
            } catch (Exception e) {
                logger.error("zk client error : " + e.getMessage());
                e.printStackTrace();
            }
            return returnPos;
        }

        private void dump() throws Exception {
            PartitionMetadata metadata = findLeader(conf.brokerSeeds, conf.portList, conf.topic, conf.partition);
            if (metadata == null) {
                logger.error("Can't find metadata for Topic and Partition. Existing");
                throw new Exception("Can't find metadata for Topic and Partition. Existing");
            }
            if (metadata.leader() == null) {
                logger.error("Can't find Leader for Topic and Partition. Existing");
                throw new Exception("Can't find Leader for Topic and Partition. Existing");
            }
            String leadBroker = metadata.leader().host();
            int leadPort = metadata.leader().port();
            String clientName = conf.clientName;
            consumer = new SimpleConsumer(leadBroker, leadPort, 100000, 64 * 1024, clientName);
            //load pos
            KafkaPosition pos = findCheckpointHBase();
            if (pos == null) {//find position failed......
                globalFetchThread = 1;
                if(consumer != null) {
                    consumer.close();
                    consumer = null;
                }
                return;
            }
            long readOffset = pos.offset;
            fetchZkOffset = readOffset;
            batchId = pos.batchId;
            inId = pos.inId;
            conf.partition = pos.partition;
            conf.topic = pos.topic;
            //perminute record
            globalOffset = pos.offset;
            globalBatchId = pos.batchId;
            globalInBatchId = pos.inId;
            globalUId = pos.uId;
            int numErr = 0;
            int counter = 0;
            fetchMonitor.fetchStart = System.currentTimeMillis();
            logger.info("init parser fetcher offset: "+readOffset+" ,batchId: "+batchId+" ,inId: "+inId+", uid: " + uId);
            timer.schedule(timerMonitor, 1000, config.monitorsec * 1000);//per minute timer task
            isFetchRunning = true;
            while (isFetch) {
                if (consumer == null) {
                    consumer = new SimpleConsumer(leadBroker, leadPort, 100000, 64 * 1024, clientName);
                }
                FetchRequest req = new FetchRequestBuilder()
                        .clientId(clientName)
                        .addFetch(conf.topic, conf.partition, readOffset, config.requestSize)
                        .build();
                FetchResponse rep = consumer.fetch(req);
                if (rep.hasError()) {
                    numErr++;
                    short code = rep.errorCode(conf.topic, conf.partition);
                    logger.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                    if (numErr >= 5) {
                        logger.error("5 errors occurred existing the fetching");
                        globalFetchThread = 1;
                        break;
                    }
                    if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                        long earliestOffset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
                        long latestOffset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                        if(readOffset > latestOffset)
                            readOffset = latestOffset;
                        else if(readOffset < earliestOffset)
                            readOffset = earliestOffset;
                        else
                            readOffset = latestOffset;
                        continue;
                    }
                    consumer.close();
                    consumer = null;
                    try {
                        leadBroker = findNewLeader(leadBroker, conf.topic, conf.partition);
                    } catch (Exception e) {
                        logger.error("find lead broker failed, " + e.getMessage(), e);
                        globalFetchThread = 1;
                        break;
                    }
                    continue;
                }
                numErr = 0;
                long numRead = 0;
                int continuousRead = 0;
                for (MessageAndOffset messageAndOffset : rep.messageSet(conf.topic, conf.partition)) {
                    long currentOffset = messageAndOffset.offset();
                    if (currentOffset < readOffset) {
                        logger.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                        continue;
                    }
                    readOffset = messageAndOffset.nextOffset();
                    ByteBuffer payload = messageAndOffset.message().payload();
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);
                    long offset = readOffset;//now msg + next offset
                    minuteMonitor.fetchNum++;
                    minuteMonitor.batchSize += bytes.length;
                    minuteMonitor.nowOffset = offset;
                    fetchMonitor.batchSize += bytes.length;
                    KafkaMetaMsg metaMsg = new KafkaMetaMsg(bytes, offset);
                    try {
                        msgQueue.put(metaMsg);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                        e.printStackTrace();
                    }
                    numRead++;
                    counter++;
                }
                if (numRead == 0) {
                    delay(1);//block
                    continuousRead ++;
                    if(continuousRead >= ParserConf.FETCH_CONTINUS_ZERO) {
                        logger.info("====================> simple consumer fetch 0 messages");
                        logger.info("-----> continuous 0 batch messages for " + continuousRead + " batches");
                        Thread.sleep(ParserConf.SLEEP_INTERNAL);
                        continuousRead = 0;
                    }
                }
                if (counter > 10000) {
                    fetchMonitor.fetchEnd = System.currentTimeMillis();
                    logger.info("===================================> fetch thread:");
                    logger.info("---> fetch thread during sum time : " + (fetchMonitor.fetchEnd - fetchMonitor.fetchStart) + " ms");
                    logger.info("---> fetch the events (bytes) sum size : " + fetchMonitor.batchSize / config.mbUnit + " MB");
                    logger.info("---> fetch the events num : " + counter + " entries");
                    fetchMonitor.clear();
                    counter = 0;
                    fetchMonitor.fetchStart = System.currentTimeMillis();
                    continuousRead = 0;
                }
            }
            logger.info("closing the simple consumer......");
            if(consumer != null) {
                consumer.close();
                consumer = null;
            }
        }

        //num / size | per minnute
        public void run() {
            try {
                dump();
            } catch (Throwable e) { // OOM exception will catch it.
                logger.error("fetcher error : " + e.getMessage(), e);
                //send monitor
                final String exmsg = e.getMessage();
                Thread sendMonitor = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ParserMonitor exMonitor = new ParserMonitor();
                            exMonitor.exMsg = exmsg;
                            JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, jobId);
                            String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                            KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                            phMonitorSender.sendKeyMsg(km);
                        } catch (Exception e1) {
                            logger.error("send exception kafka failed, " + e1.getMessage(), e1);
                        }
                    }
                });
                sendMonitor.start();
                logger.error("fetch thread encounter error:" + e.getMessage() + ",reload the job......", e);
                globalFetchThread = 1;
            }
            fetchSurvival = false;
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

                logger.info("=================================> check assembly heartbeats......");

                //run function heart beat information
                logger.info("--------> globalFetchThread : " + globalFetchThread);
                logger.info("--------> msgQueue size : " + msgQueue.size());
                logger.info("--------> fetch thread status : " + fetcher.getState());
                logger.info("--------> fetch thread alive var symbol : " + fetchSurvival);

                //check fetch thread survival
                if (!fetchSurvival) {
                    logger.info("fetch thread had been dead, reload the hob ......");
                    globalFetchThread = 1;
                    return;
                }

                /*check the thread dead*/
                if (!fetcher.isAlive()) {
                    logger.info("fetcher thread is not alive, now reload it to set the global fetcher thread  = 1 ");
                    globalFetchThread = 1;
                }
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                logger.info("heart beat lost, reloading the job......");
                globalFetchThread = 1;
            }
        }
    }

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
            while (count < ParserConf.CP_RETRY_COUNT) {
                try {
                    if (!StringUtils.isBlank(ConCP)) {
                        cpUtil.writeCp(jobId, ConCP);
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
            if(count >= ParserConf.CP_RETRY_COUNT) {
                logger.error("confirm checkpoint failed after " + count + " times, reloading the job......");
                globalFetchThread = 1;
            }
        }
    }

    //num / size / yanshi / send kafka time | per batch
    public void run() throws Exception {
        //cpu 100%
        if(msgQueue.isEmpty()) {
            delayMin(100);
        }

        //fetch thread's status
        if(globalFetchThread == 1) {
            globalFetchThread = 0;
            logger.error("zk connect failed or position is error!!! reload......");
            reload(jobId);
            delay(5);
            return;
        }

        //waiting for prepare finish , prepare's thread finish or run once
        if(!isFetchRunning) {
            delay(1);
            return;
        }

        //while heart beat
        int continuousZero = 0;

        //take the data from the queue
        while (!msgQueue.isEmpty()) {
            KafkaMetaMsg msg = msgQueue.take();
            if(msg == null) continue;
            CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(msg.msg);
            if(pre != null && !isBehind(pre,entry)) continue;//clear the repeat
            pre = entry;
            String matchTopic = getTopic(entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName());
            if(matchTopic != null) {
                CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
                entryBuilder.setHeader(entry.getHeader());
                entryBuilder.setEntryType(entry.getEntryType());
                entryBuilder.setStoreValue(entry.getStoreValue());
                entryBuilder.setBatchId(batchId);
                entryBuilder.setInId(inId);
                entryBuilder.setIp(entry.getIp());
                CanalEntry.Entry newEntry = entryBuilder.build();
                inId++;
                if(isEnd(entry)) {
                    inId=0;
                    batchId++;
                }
                entryList.add(newEntry);
                String topic = matchTopic;
                List<KeyedMessage<String, byte[]>> kms = entryToAvroToKeyed(newEntry, matchTopic);
                messageList.addAll(kms);
                monitor.topicDelay.put(topic, System.currentTimeMillis() - entry.getHeader().getExecuteTime());
            }
            //globalvar is for minute persistence, confirmOffset is for persistence
            confirmOffset = msg.offset;
//            globalOffset = msg.offset;
//            globalBatchId = batchId;
//            globalInBatchId = inId;
//            globalUId = uId;
            if(monitor.batchSize >= config.sendBatchBytes || messageList.size() >= config.batchsize) break;
        }
        //distribute data to multiple topic kafka, per time must confirm the position
        if((monitor.batchSize >= config.sendBatchBytes || messageList.size() >= config.batchsize) || (System.currentTimeMillis() - startTime) > config.timeInterval * 1000) {
            if(entryList.size() > 0){
                monitor.persisNum = messageList.size();
                last = entryList.get(entryList.size()-1);//get the last one,may be enter the time interval and size = 0
                monitor.delayTime = System.currentTimeMillis() - last.getHeader().getExecuteTime();
                monitor.topicDelay.put(ToKafkaMonitorType.DELAY_TIMES, monitor.delayTime);
            }
            if(distributeData(messageList) == -1) {
                logger.info("persistence the data failed !!! reloading ......");
                globalFetchThread = 1;
                return;
            }
            if(messageList.size() > 0) {
                confirm2Str();
                continuousZero = 0;
            } else {
                continuousZero++;
                if(continuousZero >= ParserConf.PERSIST_CONTINUS_ZERO) {
                    logger.info("============> continuous persist running 0 batch :");
                    logger.info("-----> 0 batch count = " + continuousZero);
                    logger.info("-----> sleeping " + ParserConf.SLEEP_INTERNAL);
                    Thread.sleep(ParserConf.SLEEP_INTERNAL);
                    continuousZero = 0;//reset
                }
            }
            //per minute persistence the position which must be run() persisted
            globalOffset = confirmOffset;
            globalBatchId = batchId;
            globalInBatchId = inId;
            globalUId = uId;
            entryList.clear();
            messageList.clear();
        }
        if(monitor.persisNum > 0) {
            monitor.persistenceStart = startTime;
            monitor.persistenceEnd = System.currentTimeMillis();
            logger.info("===================================> persistence thread / monitor:");
            logger.info("---> persistence deal during time:" + (monitor.persistenceEnd - monitor.persistenceStart) + " ms");
            logger.info("---> send time:" + (monitor.sendEnd - monitor.sendStart) + " ms");
            logger.info("---> delay time:" + monitor.delayTime + " ms");
            logger.info("---> the number of messages:" + monitor.persisNum + " messages");
            logger.info("---> entry list to bytes (avro) sum size is " + monitor.batchSize / config.mbUnit + " MB");
            logger.info("---> globalFetchThread :" + globalFetchThread);
            logger.info("---> queue size : " + msgQueue.size());
            logger.info("---> commit position info (not confirmed):" + " topic is " + config.topic +
                    ",partition is :" + config.partition + ",offset is :" + globalOffset + "; batch id is :" + globalBatchId +
                    ",in batch id is :" + globalInBatchId);
            if(last != null) logInfoEntry(last);
            //send phoenix monitor and through train monitor
            final ParserMonitor ptMonitor = monitor.cloneDeep();
            //final long commonDelay = System.currentTimeMillis() - last.getHeader().getExecuteTime();//bug the EntryList has been clear, the last maybe null or initial entry
            logger.info("kafka monitor time cur:" + System.currentTimeMillis() + ",time event:" + last.getHeader().getExecuteTime());
            Thread sendMonitor = new Thread(new Runnable() {
                @Override
                public void run() {
                    long commonDelay = ptMonitor.topicDelay.get(ToKafkaMonitorType.DELAY_TIMES);
                    try {
                        for(Map.Entry<String, Long> entry : ptMonitor.topicDelay.entrySet()) {
                            ptMonitor.topicDelay.put(entry.getKey(), commonDelay);
                        }
                        JrdwMonitorVo phJmv = ptMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.PERSIS_MONITOR, jobId);
                        JrdwMonitorVo thDeJmv = ptMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.TOPIC_DELAY, jobId);
                        JrdwMonitorVo thRoJmv = ptMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.TOPIC_ROWS, jobId);
                        //JrdwMonitorVo phtJmv = ptMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.TOPIC_MONITOR, jobId);
                        String phStr = JSONConvert.JrdwMonitorVoToJson(phJmv).toString();
                        String thDeStr = JSONConvert.JrdwMonitorVoToJson(thDeJmv).toString();
                        String thRoStr = JSONConvert.JrdwMonitorVoToJson(thRoJmv).toString();
                        //String phtStr = JSONConvert.JrdwMonitorVoToJson(phtJmv).toString();
                        KeyedMessage<String, byte[]> kmph = new KeyedMessage<String, byte[]>(config.phKaTopic, null, phStr.getBytes("UTF-8"));
                        KeyedMessage<String, byte[]> kmthDe = new KeyedMessage<String, byte[]>(config.phKaTopic, null, thDeStr.getBytes("UTF-8"));
                        KeyedMessage<String, byte[]> kmthRo = new KeyedMessage<String, byte[]>(config.phKaTopic, null, thRoStr.getBytes("UTF-8"));
                        //KeyedMessage<String, byte[]> kmpht = new KeyedMessage<String, byte[]>(config.phKaTopic, null, phtStr.getBytes("UTF-8"));
                        //logger.info("monitor:" + phtStr);
                        phMonitorSender.sendKeyMsg(kmph);
                        phMonitorSender.sendKeyMsg(kmthDe);
                        phMonitorSender.sendKeyMsg(kmthRo);
                        //phMonitorSender.sendKeyMsg(kmpht);
                        logger.info("monitor persistence:" + phStr);
                        logger.info("monitor topic delay:" + thDeStr);
                        logger.info("monitor topic rows:" + thRoStr);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
            sendMonitor.start();
            monitor.clear();
            startTime = System.currentTimeMillis();
        }
    }

    private int distributeData(List<KeyedMessage<String, byte[]>> msgList) {
        monitor.sendStart = System.currentTimeMillis();
        int flag = 0;
        if(msgList.size() > 0) {
            flag = msgSender.sendKeyMsg(msgList, phMonitorSender, config);//it is bug if kafka is crash we should reload the whole program when retry many times
        }
        monitor.sendEnd = System.currentTimeMillis();
        return flag;
    }

    private void confirmCheckpointHBase() throws Exception {
        String value = config.topic+":"+config.partition+":"+ confirmOffset +":"+batchId+":"+inId+":"+uId;
        cpUtil.writeCp(jobId, value);
    }

    private void confirm2Str() throws Exception {
        String value = config.topic+":"+config.partition+":"+ confirmOffset +":"+batchId+":"+inId+":"+uId;
        ConCP = value;
    }

    private void confirmPos() throws Exception {
        String value = config.topic+":"+config.partition+":"+ confirmOffset +":"+batchId+":"+inId+":"+uId;
        try {
            String zkPos = config.persisPath + "/" + jobId;
            if(!zk.exists(zkPos)) {
                zk.create(zkPos, value);
            } else {
                zk.set(zkPos, value);
            }
            zk.set(zkPos, value);
        } catch (Exception e) {
            //send monitor
            final String exmsg = e.getMessage();
            Thread sendMonitor = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        ParserMonitor exMonitor = new ParserMonitor();
                        exMonitor.exMsg = exmsg;
                        JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, jobId);
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
            boolean isZk = false;
            int retryZk = 0;
            while (!isZk) {
                if(retryZk >= config.retrys) {
                    globalFetchThread = 1;
                    return;
                }
                retryZk++;
                try {
                    String zkpos = config.persisPath + "/" + jobId;
                    zk.set(zkpos, value);
                    isZk = true;
                } catch (Exception e1) {
                    //send monitor
                    final String exmsg1 = e1.getMessage();
                    Thread sendMonitor1 = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                ParserMonitor exMonitor = new ParserMonitor();
                                exMonitor.exMsg = exmsg1;
                                JrdwMonitorVo jmv = exMonitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, jobId);
                                String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                                KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                                phMonitorSender.sendKeyMsg(km);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    sendMonitor1.start();
                    logger.error("retrying...... Exception:" + e1.getMessage());
                    delay(3);
                }
            }
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

    private String getRowChangeType(CanalEntry.RowChange row) {
        CanalEntry.EventType  type = row.getEventType();
        switch (type) {
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
    }

    private Map<String, byte[]> entryToAvro(CanalEntry.Entry entry) {
        Map<String, byte[]> maps = new LinkedHashMap<String, byte[]>();
        String div = "\u0001";
        String[] kk = config.disKey.get(entry.getHeader().getSchemaName()+"."+entry.getHeader().getTableName()).split(",");
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String getType = getRowChangeType(rowChange);
            for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                EventEntryAvro entryAvro = new EventEntryAvro();
                String keys = "";
                entryAvro.setMid(uId);
                entryAvro.setDb(entry.getHeader().getSchemaName());
                entryAvro.setSch(entry.getHeader().getSchemaName());
                entryAvro.setTab(entry.getHeader().getTableName());
                entryAvro.setOpt(getType);
                entryAvro.setTs(System.currentTimeMillis());
                if(rowChange.getIsDdl()) entryAvro.setDdl(rowChange.getSql());
                else entryAvro.setDdl("");
                entryAvro.setErr("");
                //cus mysql time and ip
                Map<CharSequence, CharSequence> cus = new HashMap<CharSequence, CharSequence>();
                cus.put(config.cusTime, String.valueOf(entry.getHeader().getExecuteTime()));
                cus.put(config.cusIp, entry.getIp());
                //current and source
                if(rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                    List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    for(CanalEntry.Column column : columns) {
                        sourceCols.put(column.getName(),column.getValue());
                        if(column.getIsKey()) {
                            currentCols.put(column.getName(),column.getValue());
                            for(String k : kk) {
                                if(k.equals(column.getName())) {
                                    keys+=column.getValue() + div;
                                    break;
                                }
                            }
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
                        if(column.getIsKey()) {
                            for(String k : kk) {//get the key's  value
                                if(k.equals(column.getName())) {
                                    keys+=column.getValue() + div;
                                    break;
                                }
                            }
                        }
                    }
                    entryAvro.setSrc(sourceCols);
                    entryAvro.setCur(currentCols);
                } else if(rowChange.getEventType() == CanalEntry.EventType.UPDATE) {
                    List<CanalEntry.Column> columnsSource = rowData.getBeforeColumnsList();
                    List<CanalEntry.Column> columnsCurrent = rowData.getAfterColumnsList();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    for(int i=0,j=0;i<=columnsCurrent.size()-1 || j<=columnsSource.size()-1;i++,j++) {
                        if(j<=columnsSource.size()-1)
                            sourceCols.put(columnsSource.get(j).getName(),columnsSource.get(j).getValue());
                        if(i<=columnsCurrent.size()-1) {
                            if (columnsCurrent.get(i).getUpdated()) {
                                currentCols.put(columnsCurrent.get(i).getName(), columnsCurrent.get(i).getValue());
                            }
                            if(columnsCurrent.get(i).getIsKey()) {
                                currentCols.put(columnsCurrent.get(i).getName(), columnsCurrent.get(i).getValue());
                                for(String k : kk) {//get the key's  value
                                    if(k.equals(columnsCurrent.get(i).getName())) {
                                        keys+=columnsCurrent.get(i).getValue() + div;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    entryAvro.setCur(currentCols);
                    entryAvro.setSrc(sourceCols);
                }
                //erase the rear "\u0001"
                keys = keys.substring(0, keys.lastIndexOf(div));
                byte[] value = getBytesFromAvro(entryAvro);
                maps.put(keys, value);
                uId++;
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return(maps);
    }

    private List<KeyedMessage<String, byte[]>> entryToAvroToKeyed(CanalEntry.Entry entry, String topic) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        String div = "\u0001";
        String getKey = config.disKey.get(entry.getHeader().getSchemaName()+"."+entry.getHeader().getTableName());
        String dt = entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
        String[] kk = null;
        if(getKey != null) kk = getKey.split(",");
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String getType = getRowChangeType(rowChange);
            if(!rowChange.getIsDdl()) { //dml
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    EventEntryAvro entryAvro = new EventEntryAvro();
                    String keys = "";
                    entryAvro.setMid(uId);
                    entryAvro.setDb(entry.getHeader().getSchemaName());
                    entryAvro.setSch(entry.getHeader().getSchemaName());
                    entryAvro.setTab(entry.getHeader().getTableName());
                    entryAvro.setOpt(getType);
                    entryAvro.setTs(System.currentTimeMillis());
                    if (rowChange.getIsDdl()) entryAvro.setDdl(rowChange.getSql());
                    else entryAvro.setDdl("");
                    entryAvro.setErr("");
                    //cus mysql time and ip
                    Map<CharSequence, CharSequence> cus = new HashMap<CharSequence, CharSequence>();
                    cus.put(config.cusTime, String.valueOf(entry.getHeader().getExecuteTime()));
                    cus.put(config.cusIp, entry.getIp());
                    entryAvro.setCus(cus);
                    //current and source
                    if (rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                        List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
                        Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                        Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                        for (CanalEntry.Column column : columns) {
                            if(isFilteredCol(dt, column.getName())) {
                                continue;
                            }
                            if(column.getIsNull()) {
                                sourceCols.put(column.getName(), null);
                            } else {
                                sourceCols.put(column.getName(), column.getValue());
                            }
                            //if (column.getIsKey()) {
                            if(column.getIsKey()) {
                                if(column.getIsNull()) {
                                    currentCols.put(column.getName(), null);
                                } else {
                                    currentCols.put(column.getName(), column.getValue());
                                }
                            }
                            for (String k : kk) {
                                if (k.equals(column.getName())) {
                                    keys += column.getValue() + div;
                                    break;
                                }
                            }
                            //}
                        }
                        entryAvro.setCur(currentCols);
                        entryAvro.setSrc(sourceCols);
                    } else if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                        List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                        Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                        Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                        for (CanalEntry.Column column : columns) {
                            if(isFilteredCol(dt, column.getName())) {
                                continue;
                            }
                            if(column.getIsNull()) {
                                currentCols.put(column.getName(), null);
                            } else {
                                currentCols.put(column.getName(), column.getValue());
                            }
                            //if (column.getIsKey()) {
                            for (String k : kk) {//get the key's  value
                                if (k.equals(column.getName())) {
                                    keys += column.getValue() + div;
                                    break;
                                }
                            }
                            //}
                        }
                        entryAvro.setSrc(sourceCols);
                        entryAvro.setCur(currentCols);
                    } else if (rowChange.getEventType() == CanalEntry.EventType.UPDATE) {
                        List<CanalEntry.Column> columnsSource = rowData.getBeforeColumnsList();
                        List<CanalEntry.Column> columnsCurrent = rowData.getAfterColumnsList();
                        Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                        Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                        for (CanalEntry.Column column : columnsSource) {
                            if(isFilteredCol(dt, column.getName())) {
                                continue;
                            }
                            if(column.getIsNull()) {
                                sourceCols.put(column.getName(), null);
                            } else {
                                sourceCols.put(column.getName(), column.getValue());
                            }
                        }
                        for (CanalEntry.Column column : columnsCurrent) {
                            if(isFilteredCol(dt, column.getName())) {
                                continue;
                            }
                            //add update value to cur
                            if (column.getUpdated()) {
                                if(column.getIsNull()) {
                                    currentCols.put(column.getName(), null);
                                } else {
                                    currentCols.put(column.getName(), column.getValue());
                                }
                            }
                            //add physical key
                            if(column.getIsKey()) {
                                if(column.getIsNull()) {
                                    currentCols.put(column.getName(), null);
                                } else {
                                    currentCols.put(column.getName(), column.getValue());
                                }
                            }
                            //set business key to monitor map and add to cur
                            for (String k : kk) {
                                if (k.equals(column.getName())) {
                                    keys += column.getValue() + div;
                                    //add to cur
                                    if(column.getIsNull()) {
                                        currentCols.put(column.getName(), null);
                                    } else {
                                        currentCols.put(column.getName(), column.getValue());
                                    }
                                    break;
                                }
                            }
                        }
                        entryAvro.setSrc(sourceCols);
                        entryAvro.setCur(currentCols);
                    } else {
                        Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                        Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                        entryAvro.setCur(currentCols);
                        entryAvro.setSrc(sourceCols);
                    }
                    //erase the rear "\u0001"
                    keys = keys.substring(0, keys.lastIndexOf(div));//bug here
                    byte[] value = getBytesFromAvro(entryAvro);
                    monitor.batchSize += value.length;
                    Long rowNum = monitor.topicRows.get(topic);
                    if(rowNum == null) rowNum = 0L;
                    rowNum++;
                    monitor.topicRows.put(topic, rowNum);
                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(topic, keys, value);
                    keyMsgs.add(km);
                    uId++;
                }
            } else {//ddl
                logger.info("=====================> encounter a ddl : ");
                logger.info("-----> ddl : " + rowChange.getSql());
//                EventEntryAvro entryAvro = new EventEntryAvro();
//                String keys = "ddl" + System.currentTimeMillis();
//                entryAvro.setMid(uId);
//                entryAvro.setDb(entry.getHeader().getSchemaName());
//                entryAvro.setSch(entry.getHeader().getSchemaName());
//                entryAvro.setTab(entry.getHeader().getTableName());
//                entryAvro.setOpt(getType);
//                entryAvro.setTs(System.currentTimeMillis());
//                if (rowChange.getIsDdl()) entryAvro.setDdl(rowChange.getSql());
//                else entryAvro.setDdl("");
//                entryAvro.setErr("");
//                //cus mysql time and ip
//                Map<CharSequence, CharSequence> cus = new HashMap<CharSequence, CharSequence>();
//                cus.put(config.cusTime, String.valueOf(entry.getHeader().getExecuteTime()));
//                cus.put(config.cusIp, entry.getIp());
//                Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
//                Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
//                entryAvro.setCur(currentCols);
//                entryAvro.setSrc(sourceCols);
//                byte[] value = getBytesFromAvro(entryAvro);
//                monitor.batchSize += value.length;
//                KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(topic, keys, value);
//                keyMsgs.add(km);
//                uId++;
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return(keyMsgs);
    }

    private boolean isFilteredCol(String dbtb, String fieldName) {
        if(config.disSenses.containsKey(dbtb)) {
            if(config.disSenses.get(dbtb).contains(fieldName))
                return true;//it is sensitive filed, filter it
            else
                return false;
        } else {
            return false;
        }
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

    private List<byte[]> getBytesFromAvro(List<EventEntryAvro> avros) {
        List<byte[]> bytes = new ArrayList<byte[]>();
        for(EventEntryAvro avro : avros) {
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
            bytes.add(value);
        }
        return bytes;
    }

    private EventEntryAvro getAvroFromBytes(byte[] value) {
        SpecificDatumReader<EventEntryAvro> reader = new SpecificDatumReader<EventEntryAvro>(EventEntryAvro.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value,null);
        EventEntryAvro avro = null;
        try {
            avro = reader.read(null,decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avro;
    }

    private boolean isBehind(CanalEntry.Entry pre, CanalEntry.Entry last) {
        if(pre.getBatchId() > last.getBatchId()) {
            return false;
        } else if(pre.getBatchId() < last.getBatchId()) {
            return true;
        } else {
            if(last.getInId() > pre.getInId()) return true;
            else return false;
        }
    }

    private String getTopic(String macher) {
        return config.disTopic.get(macher);
    }

    private boolean isEnd(CanalEntry.Entry entry) throws Exception {
        if(entry == null) return false;
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND || (entry.getEntryType() == CanalEntry.EntryType.ROWDATA && rowChange.getIsDdl())) return true;
        else return false;
    }

    private void logInfoEntry(CanalEntry.Entry lastEntry) {
        if(lastEntry != null) {
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(lastEntry.getStoreValue());
                String colValue = "null";
                if (rowChange.getRowDatasList().size() > 0) {
                    CanalEntry.RowData rowData = rowChange.getRowDatas(rowChange.getRowDatasList().size() - 1);
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

    public void pause(String id) throws Exception {

    }

    public void reload(String id) throws Exception {
        close(jobId);
        prepare(jobId);
    }

    public void close(String id) throws Exception {
        logger.info("closing the job ......");
        if(fetcher != null)
            fetcher.isFetch = false;//stop the thread
        if(fetcher.consumer != null) fetcher.consumer.close();
            fetcher.shutdown();//stop the fetcher's timer task
        if(confirm != null)
            confirm.cancel();
        if(hb != null)
            hb.cancel();
        if(htimer != null)
            htimer.cancel();
        if(timer != null)
            timer.cancel();
        if(zk != null)
            zk.close();
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
