package com.github.hackerwin7.mysql.parser.parser;

import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.parser.kafka.driver.consumer.KafkaReceiver;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaMetaMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hp on 15-3-18.
 */
public class ParserVertifyKafkaJson {

    private Logger logger = LoggerFactory.getLogger(ParserVertifyKafkaJson.class);

    private KafkaConf kcnfJson;
    private KafkaReceiver krJson;
    public boolean running = true;
    private List<byte[]> msgListJson = new ArrayList<byte[]>();

    public void loadOnlineConf() throws Exception {
        logger.info("loading conf...");
        URL url = new URL("https://raw.githubusercontent.com/hackerwin7/configuration-service/master/parser-down.properties");
        InputStream in = url.openStream();
        Properties po = new Properties();
        po.load(in);

        String zkserver = po.getProperty("kafka-zk");
        String zkroot = po.getProperty("kafka-zk-root");
        String topic = po.getProperty("kafka-topic");
        int partition = Integer.valueOf(po.getProperty("kafka-partition"));
        String dataKfkaZk = zkserver + zkroot;
        kcnfJson = new KafkaConf();
        kcnfJson.loadZk(dataKfkaZk);
        kcnfJson.partition = partition;
        kcnfJson.topic = topic;
        kcnfJson.clientName = "jsoncnf1523657";
        logger.info("json conf:" + dataKfkaZk + "," + kcnfJson.partition + "," + kcnfJson.topic + "," + kcnfJson.clientName);
        krJson = new KafkaReceiver(kcnfJson);
    }

    public void dumpJson() throws Exception {
        logger.info("dumping...");
        Thread dump = new Thread(new Runnable() {
            @Override
            public void run() {
                krJson.run();
            }
        });
        dump.start();
        while (running) {
            while (!krJson.msgQueue.isEmpty()) {
                KafkaMetaMsg kmsg = krJson.msgQueue.take();
                msgListJson.add(kmsg.msg);
                logger.info("====>json str:" + new String(kmsg.msg));
            }
        }
        msgListJson.clear();
    }

    public static void main(String[] args) throws Exception {
        ParserVertifyKafkaJson pkj = new ParserVertifyKafkaJson();
        pkj.loadOnlineConf();
        pkj.dumpJson();
    }
}
