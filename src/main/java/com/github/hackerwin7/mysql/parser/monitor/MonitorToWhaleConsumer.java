package com.github.hackerwin7.mysql.parser.monitor;

import com.jd.bdp.whale.client.ClientFactory;
import com.jd.bdp.whale.client.Configure.ClientConfigure;
import com.jd.bdp.whale.client.Consumer.DefaultConsumer;
import com.jd.bdp.whale.client.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hp on 14-9-25.
 */
public class MonitorToWhaleConsumer {

    private Logger logger = LoggerFactory.getLogger(MonitorToWhaleConsumer.class);

    private String jobId = "mysql-tracker";

    private String groupID = "mysql";

    private String topic = "magpie-eggs-topic";

    private String whaleIP = "192.168.198.87";

    private int whalePort = 10086;

    private DefaultConsumer consumer;

    private ClientFactory clientFactory;

    public void open() throws Exception {
        ClientConfigure clientConfigure = new ClientConfigure();
        clientConfigure.setManagerNode(whaleIP);
        clientConfigure.setManagerNodePort(whalePort);
        clientFactory = new ClientFactory(clientConfigure);
        MessageListener listener = new TestListener();
        consumer = clientFactory.createMessageConsumer(1, listener);
        try {
            consumer.subscribe(topic, groupID);
            consumer.completeSubscribe();
        } catch (Exception e) {
            logger.error("whale consumer subscribe error!!!");
            e.printStackTrace();
        }
    }

    static class TestListener implements MessageListener {

        private Logger logger = LoggerFactory.getLogger(TestListener.class);

        public void recieveMessages(byte[] msg) {
            logger.info("message bytes length :" + msg.length);
        }
    }

    public void close() throws Exception {
        consumer.shutdown();
        clientFactory.shutdown();
    }

}
