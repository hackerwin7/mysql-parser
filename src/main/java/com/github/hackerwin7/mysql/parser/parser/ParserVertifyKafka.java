package com.github.hackerwin7.mysql.parser.parser;

import com.github.hackerwin7.mysql.parser.kafka.driver.consumer.KafkaNoStaticReceiver;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaMetaMsg;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaNoStaticConf;
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
 * Created by hp on 14-12-31.
 */
public class ParserVertifyKafka {

    private Logger logger = LoggerFactory.getLogger(ParserVertifyKafka.class);

    private KafkaNoStaticConf kcnfAvro, kcnfJson;
    private KafkaNoStaticReceiver krAvro, krJson;
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

    private boolean isJsonEnd = false;
    private boolean isAvroEnd = false;


    private void loadOnlineConfAvroJson() throws Exception {
        String zkserver, zkroot, topic, dataKfkaZk;
        int partition;

        logger.info("load online conf");
        URL url = new URL("https://raw.githubusercontent.com/hackerwin7/configuration-service/master/parser-down.properties");
        InputStream in = url.openStream();
        Properties po = new Properties();
        po.load(in);

        zkserver = po.getProperty("kafka-zk");
        zkroot = po.getProperty("kafka-zk-root");
        topic = po.getProperty("kafka-topic");
        partition = Integer.valueOf(po.getProperty("kafka-partition"));
        dataKfkaZk = zkserver + zkroot;
        kcnfJson = new KafkaNoStaticConf();
        kcnfJson.loadZk(dataKfkaZk);
        kcnfJson.partition = partition;
        kcnfJson.topic = topic;
        kcnfJson.clientName = "jsoncnf1523657";
        krJson = new KafkaNoStaticReceiver(kcnfJson);

        zkserver = po.getProperty("kafka-avro-zk");
        zkroot = po.getProperty("kafka-avro-zk-root");
        topic = po.getProperty("kafka-avro-topic");
        partition = Integer.valueOf(po.getProperty("kafka-avro-partition"));
        dataKfkaZk = zkserver + zkroot;
        kcnfAvro = new KafkaNoStaticConf();
        kcnfAvro.loadZk(dataKfkaZk);
        kcnfAvro.partition = partition;
        kcnfAvro.topic = topic;
        kcnfAvro.clientName = "avrocnf5896532";
        krAvro = new KafkaNoStaticReceiver(kcnfAvro);

        timesize = Long.valueOf(po.getProperty("timesize")) * 60 * 1000;
        jsonKey = po.getProperty("json-key");
        jprimaryKeys = jsonKey.split(",");
        avroKey = po.getProperty("avro-key");
        aprimaryKeys = avroKey.split(",");

        logger.info("json conf:" + dataKfkaZk + "," + kcnfJson.partition + "," + kcnfJson.topic + "," + kcnfJson.clientName);
        logger.info("avro conf:" + dataKfkaZk + "," + kcnfAvro.partition + "," + kcnfAvro.topic + "," + kcnfAvro.clientName);
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
        jsonDump.start();
        avroDump.start();
        Thread jsonRec = new Thread(new Runnable() {
            private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
            @Override
            public void run() {
                logger.info("json running...");
                isJsonEnd = false;
                long starttime = System.currentTimeMillis();
                boolean isrun = true;
                logger.info("json queue...");
                while (isrun) {
                    if(System.currentTimeMillis() - starttime >= timesize) break;
                    while (isrun && !krJson.msgQueue.isEmpty()) {
                        try {
                            KafkaMetaMsg kmsg = krJson.msgQueue.take();
                            msgListJson.add(kmsg.msg);
                        } catch (Exception e) {
                            logger.error(e.getMessage());
                        }
                        if(System.currentTimeMillis() - starttime >= timesize) {
                            isrun = false;
                            break;
                        }
                    }
                }
                isJsonEnd = true;
            }
        });
        Thread avroRec = new Thread(new Runnable() {
            private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
            @Override
            public void run() {
                logger.info("avro running...");
                isAvroEnd = false;
                long starttime = System.currentTimeMillis();
                boolean isrun = true;
                logger.info("avro queue...");
                while (isrun) {
                    if(System.currentTimeMillis() - starttime >= timesize) break;
                    while (isrun && !krAvro.msgQueue.isEmpty()) {
                        try {
                            KafkaMetaMsg kmsg = krAvro.msgQueue.take();
                            msgListAvro.add(kmsg.msg);
                        } catch (Exception e) {
                            logger.error(e.getMessage());
                        }
                        if(System.currentTimeMillis() - starttime >= timesize) {
                            isrun = false;
                            break;
                        }
                    }
                }
                isAvroEnd = true;
            }
        });
        jsonRec.start();
        avroRec.start();
        logger.info("running...");
        while (!isJsonEnd || !isAvroEnd) {
            Thread.sleep(3000);
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
        logger.info("sub size :" + rkeys.size());
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
            } else {
                //logger.info("avro : map -> " + s + "," + fields.toString());
                Iterator iter = fields.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Object fkey = entry.getKey();
                    Object fvalue = entry.getValue();
                    String fk = fkey.toString();
                    //logger.info("fkfkfk:" + fk);
                    if(fk.equals(s)) {
                        keyStr += fvalue.toString() + "#";
                        break;
                    }
                }
            }
        }
        keyStr += oper;
        return keyStr;
    }

    private String getJsonKey(byte[] value) throws Exception {
        String keyStr = "";
        String json = new String(value, "UTF-8");
        JSONObject jo = JSONObject.fromObject(json);
        String sdata = jo.getString("data");
        JSONObject jdata = JSONObject.fromObject(sdata);
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
        ParserVertifyKafka par = new ParserVertifyKafka();
        par.loadOnlineConfAvroJson();
        par.dumpJsonAvro();
    }
}
