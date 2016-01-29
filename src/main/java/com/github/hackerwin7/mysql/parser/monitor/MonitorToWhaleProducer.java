package com.github.hackerwin7.mysql.parser.monitor;

import com.jd.bdp.magpie.queue.utils.JSonUtils;
import com.jd.bdp.whale.client.ClientFactory;
import com.jd.bdp.whale.client.Configure.ClientConfigure;
import com.jd.bdp.whale.client.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 14-9-25.
 */
public class MonitorToWhaleProducer {

    private Logger logger = LoggerFactory.getLogger(MonitorToWhaleProducer.class);

    private String jobId = "mysql-tracker";

    private String topic = "magpie-eggs-topic";

    private String whaleIP = "192.168.198.87";

    private int whalePort = 10086;

    private MessageProducer producer;

    private ClientFactory clientFactory;

    public void open() throws Exception{
        ClientConfigure clientConfigure = new ClientConfigure();
        clientConfigure.setManagerNode(whaleIP);
        clientConfigure.setManagerNodePort(whalePort);
        clientFactory = new ClientFactory(clientConfigure);
        producer = clientFactory.createMessageProducer(1);
        producer.publish(topic);
    }

    public void send(int monitorType, String message) throws Exception{
        Map<String, String> sendMap = new HashMap<String, String>();
        sendMap.put("ID", jobId);
        sendMap.put("MonitorType", String.valueOf(monitorType));
        sendMap.put("Message", message);
        producer.sendMessage(JSonUtils.object2Json(monitorType).getBytes());
        logger.info("monitor send to whale : " +
                        "job id -> " + jobId + "," +
                        "monitor type -> " + monitorType + "," +
                        "message -> " + message

        );
    }

    public void close() throws Exception{
        producer.shutdown();
        clientFactory.shutdown();
    }


}
