package com.github.hackerwin7.mysql.parser.parser.utils;

import com.github.hackerwin7.jd.lib.utils.SiteConfService;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.parser.protocol.json.ConfigJson;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Created by hp on 14-12-15.
 */
public class ParserConf {
    private Logger logger = LoggerFactory.getLogger(ParserConf.class);

    //kafka conf
    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int kafkaPort = 9092;
    public static String zkKafka = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String keySerializer = "kafka.serializer.StringEncoder";
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public        String kafkaCompression = "snappy";
    public static String acks = "-1";
    public static String topic = "test";//queue topic
    public static String senderTopic = "test2";
    public static int partition = 0;
    public static String strSeeds = "127.0.0.1";//"172.17.36.53,172.17.36.54,172.17.36.55";
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    public static List<Integer> portList = new ArrayList<Integer>();
    public static String clientName;
    //zk conf
    public String zkServers = "127.0.0.1:2181";//"48:2181,19:2181,50:2181"
    public int timeout = 100000;
    public String rootPath = "/checkpoint";
    public String persisPath = rootPath + "/persistence";
    public String minutePath = rootPath + "/minutes";
    //parser conf <database.table, topic> database.table could be a regex
    public int batchsize = 50000; //default is 10000
    public int queuesize = 80000; //default is 20000
    public int minsec = 1 * 60;
    public int heartsec = 1 * 60;
    public int timeInterval = 1;
    public int reInterval = 3;//retry 3 seconds to retry and reconnect
    public int retrys = 2;// 0 1 2, 3 times retry for zk
    public int conUnit = 1024;//1024 bytes
    public int fetchKb = 1024;//1024 Kb
    public int requestSize = 5 * fetchKb * conUnit;//1 * fetchKb * conUnit;// default is 1 MB  (fetch size)
    public double mbUnit = 1024.0 * 1024.0;
    public String jobId = "mysql-parser";
    public int spacesize = 8;//15 MB
    public int monitorsec = 60;//1 minute
    public long mid = 0;
    //filter
    public static Map<String, String> disTopic = new HashMap<String, String>();
    public static Map<String, String> disKey = new HashMap<String, String>();
    public static Map<String, String> disType = new HashMap<String, String>();
    public Map<String, Set<String>> disSenses = new HashMap<>();
    //avro
    public String cusTime = "dmlts";
    public String cusIp = "ip";
    //phenix monitor
    public String phKaBrokerList = "localhost:9092";
    public int phKaPort = 9092;
    public String phKaZk = "localhost:2181";
    public String phKaSeria = "kafka.serializer.DefaultEncoder";
    public String phKaKeySeria = "kafka.serializer.StringEncoder";
    public String phKaParti = "kafka.producer.DefaultPartitioner";
    public String phKaAcks = "1";
    public String phKaTopic = "test1";
    public int phKaPartition = 0;
    public String CLASS_PREFIX = "classpath:";
    //constants
    public static final int FETCH_CONTINUS_ZERO = 50;
    public static final long SLEEP_INTERNAL = 3000;
    public static final int PERSIST_CONTINUS_ZERO = 50;
    public static final int CP_RETRY_COUNT = 3;
    public static final long HEART_BEAT_TIMER_INTERVAL = 60 * 1000;//60s
    public static final long CONFIRM_TIMER_INTERVAL = 60 * 1000;
    public static final long HEART_BEAT_DELAY_START = 60 * 1000;
    public static final long CONFIRM_DELAY_START = 10 * 1000;//after 10 seconds, start the confirm timer task
    public static final long KAFKA_SEND_DEFAULT_BATCH_BYTES = 1024 * 1024;
    public static final long KAFKA_SEND_COMPRESS_BATCH_BYTES = 1000 * 1024;
    public static final long KAFKA_SEND_UNCOMPRESSION_BATCH_BYTES = 10 * 1024 * 1024;
    public static final int KAFKA_RECONN_COUNT = 1;
    public static final int KAFKA_RETRY_COUNT = 3;
    //kafka batch send
    public long sendBatchBytes = KAFKA_SEND_DEFAULT_BATCH_BYTES;

    public void initConfLocal() {
        brokerSeeds.add("127.0.0.1");
        disTopic.put("canal_test.simple", "aa");
        disTopic.put("canal_test.test", "bb");
        disTopic.put("canal_test.filter", "aa");
    }

    public void initConfStatic() {
        brokerList = "172.17.36.53:9092,172.17.36.54:9092,172.17.36.55:9092";
        String ss[] = strSeeds.split(",");
        for(String s : ss) {
            brokerSeeds.add(s);
        }
        kafkaPort = 9092;
        zkKafka = "172.17.36.60/kafka";
        topic = "mysql_log";
        zkServers = "172.17.36.60:2181,172.17.36.61:2181,172.17.36.62:2181";
    }

    public void initConfFile() throws Exception {
        clear();
        String cnf = System.getProperty("parser.conf", "classpath:parser.properties");
        logger.info("load file : " + cnf);
        InputStream in = null;
        if(cnf.startsWith(CLASS_PREFIX)) {
            cnf = StringUtils.substringAfter(cnf, CLASS_PREFIX);
            in = ParserConf.class.getClassLoader().getResourceAsStream(cnf);
        } else {
            in = new FileInputStream(cnf);
        }
        Properties pro = new Properties();
        pro.load(in);
        //load the parameters
        jobId = pro.getProperty("job.name");
        logger.info("job Id : " + jobId);
        String dataKafkaZk = pro.getProperty("kafka.data.zkserver") + pro.getProperty("kafka.data.zkroot");
        logger.info("data kafka zk :" + dataKafkaZk);
        KafkaConf dataCnf = new KafkaConf();
        dataCnf.loadZk(dataKafkaZk);
        brokerList = dataCnf.brokerList;
        brokerSeeds.addAll(dataCnf.brokerSeeds);
        portList.addAll(dataCnf.portList);
        topic = pro.getProperty("kafka.data.tracker.topic");
        senderTopic = pro.getProperty("kafka.data.parser.topic");
        clientName = pro.getProperty("kafka.data.client.name");
        acks = pro.getProperty("kafka.acks");
        String monitorKafkaZk = pro.getProperty("kafka.monitor.zkserver") + pro.getProperty("kafka.monitor.zkroot");
        KafkaConf monitorCnf = new KafkaConf();
        monitorCnf.loadZk(monitorKafkaZk);
        phKaBrokerList = monitorCnf.brokerList;
        phKaTopic = pro.getProperty("kafka.monitor.topic");
        zkServers = pro.getProperty("zookeeper.servers");
        in.close();
    }

    public void initConfJSON() {
        //load magpie json
        ConfigJson jcnf = new ConfigJson(jobId, "magpie.address");
        JSONObject root = jcnf.getJson();
        //parser json
        if(root != null) {
            JSONObject data = root.getJSONObject("info").getJSONObject("content");
            brokerList = data.getString("brokerList");
            strSeeds = data.getString("strSeeds");
            String ss[] = strSeeds.split(",");
            for(String s : ss) {
                brokerSeeds.add(s);
            }
            kafkaPort = Integer.valueOf(data.getString("kafkaPort"));
            zkKafka = data.getString("zkKafka");
            topic = data.getString("topic");
            zkServers = data.getString("zkServers");
        }

        //load fileter json
        ConfigJson fcnf = new ConfigJson("", "kafka.distributed.address");
        JSONArray froot = fcnf.getJsonArr();
        //parser json
        if(froot != null) {
            for(int i = 0; i <= froot.size() - 1; i++) {
                JSONObject data = froot.getJSONObject(i);
                String dbname = data.getString("dbname");
                String tablename = data.getString("tablename");
                String mapkey = dbname + "." + tablename;
                String primarykey = data.getString("primarykey");
                if(primarykey != null) disKey.put(mapkey, primarykey);
                String sourceType = data.getString("sourceType");
                if(sourceType != null) disType.put(mapkey, sourceType);
                String topic = data.getString("topic");
                if(topic != null) disTopic.put(mapkey, topic);
            }
        }
    }

    public void initConfOnlineJSON() throws Exception {
        clear();
        ConfigJson jcnf = new ConfigJson(jobId, "release.address");
        JSONObject root = jcnf.getJson();
        //parse the json
        if(root != null) {
            int _code = root.getInt("_code");
            if(_code != 0) {
                String errMsg = root.getString("errorMessage");
                throw new Exception(errMsg);
            }
            JSONObject data = root.getJSONObject("data");
            //jobId
            jobId = data.getString("job_id");
            //get kafka parameter from zk data kafka of consumer
            String dataKafkaZk = data.getString("kafka_zkserver") + data.getString("kafka_zkroot");
            KafkaConf dataCnf = new KafkaConf();
            dataCnf.loadZk(dataKafkaZk);
            brokerList = dataCnf.brokerList;
            brokerSeeds.addAll(dataCnf.brokerSeeds);
            portList.addAll(dataCnf.portList);
            //simple get json to kafka
            topic = data.getString("tracker_log_topic");
            clientName = data.getString("kafka_clientname");
            acks = data.getString("kafka_acks");
            //get kafka monitor parameter from zk monitor of kafka producer
            String monitorKafkaZk = data.getString("monitor_server") + data.getString("monitor_zkroot");
            KafkaConf monitorCnf = new KafkaConf();
            monitorCnf.loadZk(monitorKafkaZk);
            phKaBrokerList = monitorCnf.brokerList;
            //simple get topic
            phKaTopic = data.getString("monitor_topic");
            //own zk
            zkServers = data.getString("offset_zkserver");
            //get parser filter parameter
            if(data.containsKey("db_tab_meta")) {
                JSONArray jf = data.getJSONArray("db_tab_meta");
                for (int i = 0; i <= jf.size() - 1; i++) {
                    JSONObject jdata = jf.getJSONObject(i);
                    String dbname = jdata.getString("dbname");
                    String tablename = jdata.getString("tablename");
                    String mapkey = dbname + "." + tablename;
                    String primarykey = jdata.getString("primarykey");
                    if (primarykey != null) disKey.put(mapkey, primarykey);
                    String sourceType = jdata.getString("sourceType");
                    if (sourceType != null) disType.put(mapkey, sourceType);
                    String topic = jdata.getString("topic");
                    if (topic != null) disTopic.put(mapkey, topic);
                    if(jdata.containsKey("sensitive")) {//load sensitive config
                        String senseStr = jdata.getString("sensitive");
                        String[] senseArr = StringUtils.split(senseStr, ",");
                        Set<String> valueSet = new HashSet<>();
                        for(String value : senseArr) {
                            valueSet.add(value);
                        }
                        disSenses.put(mapkey, valueSet);//put into the collection
                    }
                }
            }
            //get mid configuration
            if(data.containsKey("mid")) {
                mid = Long.valueOf(data.getString("mid"));
            }
        }
        //if compress set batch size
        if(!StringUtils.equalsIgnoreCase(kafkaCompression, "none"))
            sendBatchBytes = KAFKA_SEND_COMPRESS_BATCH_BYTES;
        else
            sendBatchBytes = KAFKA_SEND_UNCOMPRESSION_BATCH_BYTES;
    }

    /**
     * read the config from the site service interface
     * @throws Exception
     */
    public void initConfigFromSiteService() throws Exception {
        clear();
        SiteConfService scs = new SiteConfService();
        String val = scs.read(jobId);
        //parse the json
        JSONObject data = JSONObject.fromObject(val);
        //jobId
        jobId = data.getString("job_id");
        //get kafka parameter from zk data kafka of consumer
        String dataKafkaZk = data.getString("kafka_zkserver") + data.getString("kafka_zkroot");
        KafkaConf dataCnf = new KafkaConf();
        dataCnf.loadZk(dataKafkaZk);
        brokerList = dataCnf.brokerList;
        brokerSeeds.addAll(dataCnf.brokerSeeds);
        portList.addAll(dataCnf.portList);
        //simple get json to kafka
        topic = data.getString("tracker_log_topic");
        clientName = data.getString("kafka_clientname");
        acks = data.getString("kafka_acks");
        //get kafka monitor parameter from zk monitor of kafka producer
        String monitorKafkaZk = data.getString("monitor_server") + data.getString("monitor_zkroot");
        KafkaConf monitorCnf = new KafkaConf();
        monitorCnf.loadZk(monitorKafkaZk);
        phKaBrokerList = monitorCnf.brokerList;
        //simple get topic
        phKaTopic = data.getString("monitor_topic");
        //own zk
        zkServers = data.getString("offset_zkserver");
        //get parser filter parameter
        if(data.containsKey("db_tab_meta")) {
            JSONArray jf = data.getJSONArray("db_tab_meta");
            for (int i = 0; i <= jf.size() - 1; i++) {
                JSONObject jdata = jf.getJSONObject(i);
                String dbname = jdata.getString("dbname");
                String tablename = jdata.getString("tablename");
                String mapkey = dbname + "." + tablename;
                String primarykey = jdata.getString("primarykey");
                if (primarykey != null) disKey.put(mapkey, primarykey);
                String sourceType = jdata.getString("sourceType");
                if (sourceType != null) disType.put(mapkey, sourceType);
                String topic = jdata.getString("topic");
                if (topic != null) disTopic.put(mapkey, topic);
                if(jdata.containsKey("sensitive")) {//load sensitive config
                    String senseStr = jdata.getString("sensitive");
                    String[] senseArr = StringUtils.split(senseStr, ",");
                    Set<String> valueSet = new HashSet<>();
                    for(String value : senseArr) {
                        valueSet.add(value);
                    }
                    disSenses.put(mapkey, valueSet);//put into the collection
                }
            }
        }
        //get mid configuration
        if(data.containsKey("mid")) {
            mid = Long.valueOf(data.getString("mid"));
        }
        kafkaCompression = "none";
        //if compress set batch size
        if(!StringUtils.equalsIgnoreCase(kafkaCompression, "none"))
            sendBatchBytes = KAFKA_SEND_COMPRESS_BATCH_BYTES;
        else
            sendBatchBytes = KAFKA_SEND_UNCOMPRESSION_BATCH_BYTES;
    }

    public void clear() {
        brokerSeeds.clear();
        disTopic.clear();
        disKey.clear();
        disType.clear();
        disSenses.clear();
    }
}
