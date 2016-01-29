package com.github.hackerwin7.mysql.parser.kafka.driver.producer;

import com.github.hackerwin7.mysql.parser.protocol.json.JSONConvert;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.github.hackerwin7.mysql.parser.kafka.utils.KafkaConf;
import com.github.hackerwin7.mysql.parser.monitor.JrdwMonitorVo;
import com.github.hackerwin7.mysql.parser.monitor.ParserMonitor;
import com.github.hackerwin7.mysql.parser.monitor.constants.JDMysqlParserMonitorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.hackerwin7.mysql.parser.parser.utils.ParserConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaSender {

    private Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    private KafkaConf conf;
    private Producer<String, byte[]> producer;
    private int retrys = ParserConf.KAFKA_RECONN_COUNT;
    private int reconns = ParserConf.KAFKA_RETRY_COUNT;

    public KafkaSender(KafkaConf cf) {
        conf = cf;
    }

    public void connect() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("key.serializer.class", conf.keySerializer);
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        prop.put("compression.codec", conf.compression);
        ProducerConfig pConfig = new ProducerConfig(prop);
        producer = new Producer<String, byte[]>(pConfig);
    }

    public void send(byte[] msg) {
        KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(conf.topic, null, msg);
        blockSend(keyMsg);
    }

    public void send(String topic, byte[] msg) {
        KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(topic, null, msg);
        blockSend(keyMsg);
    }

    public void send(List<byte[]> msgs) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        for(byte[] msg : msgs) {
            KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(conf.topic, null, msg);
            keyMsgs.add(keyMsg);
        }
        blockSend(keyMsgs);
    }

    public void send(String topic, List<byte[]> msgs) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        for(byte[] msg : msgs) {
            KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(topic, null, msg);
            keyMsgs.add(keyMsg);
        }
        blockSend(keyMsgs);
    }

    public int sendKeyMsg(List<KeyedMessage<String, byte[]>> keyMsgs) {
        return blockSend(keyMsgs);
    }

    public int sendKeyMsg(List<KeyedMessage<String, byte[]>> keyMsgs, KafkaSender sender, ParserConf config) {
        return blockSend(keyMsgs, sender, config);
    }

    public int sendKeyMsg(KeyedMessage<String, byte[]> km) {
        return blockSend(km);
    }

    public int sendKeyMsg(KeyedMessage<String, byte[]> km, KafkaSender sender, ParserConf config) {
        return blockSend(km, sender, config);
    }

    public int blockSend(List<KeyedMessage<String, byte[]>> keyMsgs) {
        boolean isAck = false;
        int retryKafka = 0;
        int reconnKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                reconnKafka++;
                if(reconnKafka > reconns) {
                    return -1;
                }
                logger.error("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsgs);
                isAck = true;
            } catch (Exception e) {
                logger.error("retrying sending... Exception:" + e.getMessage(), e);
                delay(3);
            }
        }
        return 0;
    }

    public int blockSend(List<KeyedMessage<String, byte[]>> keyMsgs, KafkaSender sender, ParserConf config) {
        boolean isAck = false;
        int retryKafka = 0;
        int reconnKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                reconnKafka++;
                if(reconnKafka > reconns) {
                    return -1;
                }
                logger.error("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsgs);
                isAck = true;
            } catch (Exception e) {
                //send monitor
                try {
                    ParserMonitor monitor = new ParserMonitor();
                    monitor.exMsg = e.getMessage();
                    JrdwMonitorVo jmv = monitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, config.jobId);
                    String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                    sender.sendKeyMsg(km);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                logger.error("retrying sending... Exception:" + e.getMessage(), e);
                delay(3);
            }
        }
        return 0;
    }

    public int blockSend(KeyedMessage<String, byte[]> keyMsg) {
        boolean isAck = false;
        int retryKafka = 0;
        int reconnKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                reconnKafka++;
                if(reconnKafka > reconns) {
                    return -1;
                }
                logger.error("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsg);
                isAck = true;
            } catch (Exception e) {
                logger.error("retrying sending... Exception:" + e.getMessage(), e);
                delay(3);
            }
        }
        return 0;
    }

    public int blockSend(KeyedMessage<String, byte[]> keyMsg, KafkaSender sender, ParserConf config) {
        boolean isAck = false;
        int retryKafka = 0;
        int reconnKafka = 0;
        while (!isAck) {
            if(retryKafka >= retrys) {
                reconnect();
                reconnKafka++;
                if(reconnKafka > reconns) {
                    return -1;
                }
                logger.error("retry times out, reconnect the kafka server......");
                retryKafka = 0;
            }
            retryKafka++;
            try {
                producer.send(keyMsg);
                isAck = true;
            } catch (Exception e) {
                //send monitor
                try {
                    ParserMonitor monitor = new ParserMonitor();
                    monitor.exMsg = e.getMessage();
                    JrdwMonitorVo jmv = monitor.toJrdwMonitorOnline(JDMysqlParserMonitorType.EXCEPTION_MONITOR, config.jobId);
                    String jsonStr = JSONConvert.JrdwMonitorVoToJson(jmv).toString();
                    KeyedMessage<String, byte[]> km = new KeyedMessage<String, byte[]>(config.phKaTopic, null, jsonStr.getBytes("UTF-8"));
                    sender.sendKeyMsg(km);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                logger.error("retrying sending... Exception:" + e.getMessage(), e);
                delay(3);
            }
        }
        return 0;
    }

    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if(producer != null) producer.close();
    }

    public void reconnect() {
        close();
        connect();
    }

    public boolean isConnected() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("key.serializer.class", conf.keySerializer);
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        prop.put("send.buffer.bytes",  conf.sendBufferSize);
        ProducerConfig pConfig = new ProducerConfig(prop);
        Producer<String, byte[]> heartPro = null;
        try {
            heartPro = new Producer<String, byte[]>(pConfig);
            if(heartPro != null) heartPro.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
