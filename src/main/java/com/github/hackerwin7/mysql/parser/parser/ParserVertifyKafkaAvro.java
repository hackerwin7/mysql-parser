package com.github.hackerwin7.mysql.parser.parser;

import com.github.hackerwin7.mysql.parser.kafka.driver.consumer.KafkaReceiver;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaMetaMsg;
import net.sf.json.JSONObject;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.hackerwin7.mysql.parser.protocol.avro.EventEntryAvro;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by hp on 15-3-17.
 */
public class ParserVertifyKafkaAvro {

    private Logger logger = LoggerFactory.getLogger(ParserVertifyKafkaAvro.class);

    private KafkaConf kcnf, kcnfAvro, kcnfJson;
    private KafkaReceiver kr, krAvro, krJson;
    public boolean running = true;
    private List<byte[]> msgList = new ArrayList<byte[]>();
    private List<byte[]> msgListAvro = new ArrayList<byte[]>();
    private List<byte[]> msgListJson = new ArrayList<byte[]>();
    private long timesize = 0;
    private String jsonKey;
    private String avroKey;
    private String[] jprimaryKeys;
    private String[] aprimaryKeys;
    private Set<String> jkeys = new HashSet<String>();
    private Set<String> akeys = new HashSet<String>();
    private Set<String> rkeys = new HashSet<String>();

    public ParserVertifyKafkaAvro(KafkaConf kc) {
        kcnf = kc;
        kr = new KafkaReceiver(kcnf);
    }

    public ParserVertifyKafkaAvro() {

    }

    private void initJson() throws Exception {
        kcnf = new KafkaConf();
        loadOnlineConf();
        kr = new KafkaReceiver(kcnf);
    }

    private void initAvro() throws Exception {
        kcnf = new KafkaConf();
        loadOnlineAvro();
        kr = new KafkaReceiver(kcnf);
    }


    private void loadOnlineConfAvroJson() throws Exception {
        logger.info("load online conf");
        URL url = new URL("https://raw.githubusercontent.com/hackerwin7/configuration-service/master/parser-down.properties");
        InputStream in = url.openStream();
        Properties po = new Properties();
        po.load(in);
        String zkserver = po.getProperty("kafka-zk");
        String zkroot = po.getProperty("kafka-zk-root");
        String topic = po.getProperty("kafka-topic");
        int partition = Integer.valueOf(po.getProperty("kafka-partition"));

        String dataKfkaZk = zkserver + zkroot;
        kcnfJson.loadZk(dataKfkaZk);
        kcnfJson.partition = partition;
        kcnfJson.topic = topic;

        krJson = new KafkaReceiver(kcnfJson);

        zkserver = po.getProperty("kafka-avro-zk");
        zkroot = po.getProperty("kafka-avro-zk-root");
        topic = po.getProperty("kafka-avro-topic");
        partition = Integer.valueOf(po.getProperty("kafka-avro-partition"));

        dataKfkaZk = zkserver + zkroot;
        kcnfAvro.loadZk(dataKfkaZk);
        kcnfAvro.partition = partition;
        kcnfAvro.topic = topic;

        krAvro = new KafkaReceiver(kcnfAvro);

        timesize = Long.valueOf(po.getProperty("timesize")) * 60 * 1000;

        jsonKey = po.getProperty("json-key");
        jprimaryKeys = jsonKey.split(",");
        avroKey = po.getProperty("avro-key");
        aprimaryKeys = avroKey.split(",");
    }

    private void loadStatic() {
//        kcnf.brokerSeeds.add("172.17.36.53");
//        kcnf.brokerSeeds.add("172.17.36.54");
//        kcnf.brokerSeeds.add("172.17.36.55");
//        kcnf.port = 9092;
//        kcnf.portList.add(9092);
//        kcnf.portList.add(9092);
//        kcnf.portList.add(9092);
//        kcnf.partition = 0;
//        kcnf.topic = "mysql_bb";

        kcnf.brokerSeeds.add("127.0.0.1");
        kcnf.portList.add(9092);
        kcnf.partition = 0;
        kcnf.topic = "parser-log-mysql";
    }

    private void loadOnlineConf() throws Exception {
        logger.info("load online conf");
        URL url = new URL("https://raw.githubusercontent.com/hackerwin7/configuration-service/master/parser-down.properties");
        InputStream in = url.openStream();
        Properties po = new Properties();
        po.load(in);
        String zkserver = po.getProperty("kafka-zk");
        String zkroot = po.getProperty("kafka-zk-root");
        String topic = po.getProperty("kafka-topic");
        int partition = Integer.valueOf(po.getProperty("kafka-partition"));

        String dataKfkaZk = zkserver + zkroot;
        kcnf.loadZk(dataKfkaZk);
        kcnf.partition = partition;
        kcnf.topic = topic;
    }

    private void loadOnlineAvro() throws Exception {
        logger.info("load online conf");
        URL url = new URL("https://raw.githubusercontent.com/hackerwin7/configuration-service/master/parser-down.properties");
        InputStream in = url.openStream();
        Properties po = new Properties();
        po.load(in);
        String zkserver = po.getProperty("kafka-avro-zk");
        String zkroot = po.getProperty("kafka-avro-zk-root");
        String topic = po.getProperty("kafka-avro-topic");
        int partition = Integer.valueOf(po.getProperty("kafka-partition"));

        String dataKfkaZk = zkserver + zkroot;
        kcnf.loadZk(dataKfkaZk);
        kcnf.partition = partition;
        kcnf.topic = topic;
    }

    public void dumpJsonAvro() throws Exception {
        logger.info("dumping...");
        Thread jsonDump = new Thread(new Runnable() {
            @Override
            public void run() {
                krJson.run();
            }
        });
        Thread avroDump = new Thread(new Runnable() {
            @Override
            public void run() {
                krAvro.run();
            }
        });
        long starttime = System.currentTimeMillis();
        jsonDump.start();
        avroDump.start();
        while (running) {
            if(System.currentTimeMillis() - starttime >= timesize) break;
            while (running && !krJson.msgQueue.isEmpty()) {
                KafkaMetaMsg kmsg = krJson.msgQueue.take();
                msgListJson.add(kmsg.msg);
                logger.info(new String(kmsg.msg));
                if(System.currentTimeMillis() - starttime >= timesize) {
                    running = false;
                    break;
                }
            }
            while (running && !krAvro.msgQueue.isEmpty()) {
                KafkaMetaMsg kmsg = krAvro.msgQueue.take();
                msgListAvro.add(kmsg.msg);
                logger.info(new String(kmsg.msg));
                if(System.currentTimeMillis() - starttime >= timesize) {
                    running = false;
                    break;
                }
            }
        }
        logger.info("size :" + msgListJson.size() + ", " + msgListAvro.size());
        //do some operation
        jkeys.clear();
        akeys.clear();
        rkeys.clear();
        for(byte[] value : msgListJson) {
            String key = getJsonKey(value);
            jkeys.add(key);
            logger.info("json keys :" + key);
        }
        for(byte[] value : msgListAvro) {
            String key = getAvroKey(value);
            akeys.add(key);
            logger.info("avro keys :" + key);
        }
        rkeys.addAll(jkeys);
        rkeys.removeAll(akeys);
        for(String subKey : rkeys) {
            logger.info("sub set key : " + subKey);
        }
        logger.info("closed...");
    }

    private String getAvroKey(byte[] value) {
        String keyStr = "";
        EventEntryAvro avro = getAvroFromBytes(value);
        String dbname = avro.getDb().toString();
        String tbname = avro.getTab().toString();
        String oper = avro.getOpt().toString();
        keyStr += dbname + "#" + tbname + "#";
        Map<CharSequence, CharSequence> fields = avro.getCur();
        for(String s : aprimaryKeys) {
            if(fields.containsKey(s)) {
                String kv = fields.get(s).toString();
                keyStr += kv + "#";
            }
        }
        keyStr += oper;
        return keyStr;
    }

    private String getJsonKey(byte[] value) {
        String keyStr = "";
        String jsonStr = new String(value);
        JSONObject jo = JSONObject.fromObject(jsonStr);
        JSONObject jdata = jo.getJSONObject("data");
        JSONObject jfields = jdata.getJSONObject("fields");
        String dbname = jdata.getString("schema");
        String tbname = jdata.getString("table");
        String oper = jdata.getString("operation");
        keyStr += dbname + "#" +tbname + "#";
        for(String s : jprimaryKeys) {
            if(jfields.containsKey(s)) {
                String kv = jfields.getString(s);
                keyStr += kv + "#";
            }
        }
        keyStr += oper;
        return keyStr;
    }

    public void dumpJson() throws Exception {
        logger.info("dumping...");
        //thread start dumping
        Thread tdump = new Thread(new Runnable() {
            @Override
            public void run() {
                kr.run();
            }
        });
        tdump.start();
        while (running) {
            while (!kr.msgQueue.isEmpty()) {
                KafkaMetaMsg kmsg = kr.msgQueue.take();
                msgList.add(kmsg.msg);
            }
            for(byte[] value : msgList) {
                String sv = new String(value);
                logger.info("value is :" + sv);
            }
            msgList.clear();
        }
    }

    public void dumpAvro() throws Exception {
        logger.info("dumping...");
        //thread start dumping
        Thread tdump = new Thread(new Runnable() {
            @Override
            public void run() {
                kr.run();
            }
        });
        tdump.start();
        while (running) {
            while (!kr.msgQueue.isEmpty()) {
                KafkaMetaMsg kmsg = kr.msgQueue.take();
                msgList.add(kmsg.msg);
            }
            for(byte[] value : msgList) {
                EventEntryAvro avro = getAvroFromBytes(value);
                logger.info("================================================= get message :");
                logger.info("---> dbName:"+avro.getDb()+",tableName:"+avro.getTab());
                logger.info("---> type:"+avro.getOpt()+",ddl:"+avro.getDdl());
                logger.info("---> cus:"+getMapVal(avro.getCus()));
                logger.info("---> column/value:" + getColVal(avro.getCur()));
            }
            msgList.clear();
        }
    }

    private String getMapVal(Map<CharSequence, CharSequence> cv) {
        String constr = "";
        if(cv != null) {
            Iterator iter = cv.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                constr += ("[" + key.toString() + "," + value.toString() + "]");
            }
        }
        return constr;
    }

    private String getColVal(Map<CharSequence, CharSequence> cv) {
        String constr = "";
        if(cv != null) {
            Iterator iter = cv.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                constr += ("[" + key.toString() + "," + value.toString() + "]");
            }
        }
        return constr;
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

    public static void main(String[] args) throws Exception {
        ParserVertifyKafkaAvro par = new ParserVertifyKafkaAvro();
        par.initAvro();
        par.dumpAvro();
    }

}
