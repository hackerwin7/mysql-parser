package com.github.hackerwin7.mysql.parser.monitor;

import com.github.hackerwin7.mysql.parser.protocol.json.JSONConvert;
import com.github.hackerwin7.mysql.parser.monitor.constants.JDMysqlParserMonitor;
import com.github.hackerwin7.mysql.parser.monitor.constants.JDMysqlParserMonitorType;
import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 14-9-23.
 */
public class ParserMonitor implements Cloneable {

    public long fetchStart;

    public long fetchEnd;

    public long persistenceStart;

    public long persistenceEnd;

    public long sendStart;

    public long sendEnd;

    public long perMinStart;

    public long perMinEnd;

    public long hbaseReadStart;

    public long hbaseReadEnd;

    public long hbaseWriteStart;

    public long hbaseWriteEnd;

    public long serializeStart;

    public long serializeEnd;

    public long fetchNum;

    public long persisNum;

    public long batchSize;//bytes for unit

    public long fetcherStart;

    public long fetcherEnd;

    public long decodeStart;

    public long decodeEnd;

    public long delayTime;

    public long nowOffset;

    public long lastOffset;

    public Map<String, Long> topicDelay;

    public Map<String, Long> topicRows;

    public String exMsg;

    public String ip;

    public ParserMonitor() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
        nowOffset = lastOffset = 0;
        topicDelay = new HashMap<String, Long>();
        topicRows = new HashMap<String, Long>();
        exMsg = ip = "";
    }

    public Object clone() {
        Object o = null;
        try {
            ParserMonitor os = (ParserMonitor) super.clone();
            os.topicDelay = new HashMap<String, Long>();
            os.topicRows = new HashMap<String, Long>();
            if(topicDelay != null) {
                for(Map.Entry<String, Long> entry : topicDelay.entrySet()) {
                    String key = entry.getKey();
                    Long value = entry.getValue();
                    os.topicDelay.put(key, value);
                }
            }
            if(topicRows != null) {
                for(Map.Entry<String, Long> entry : topicRows.entrySet()) {
                    String key = entry.getKey();
                    Long value = entry.getValue();
                    os.topicRows.put(key, value);
                }
            }
            o = (ParserMonitor) os;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }

    public ParserMonitor cloneDeep() {
        return (ParserMonitor) clone();
    }

    public void clear() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
        nowOffset = lastOffset = 0;
        exMsg = ip = "";
        if(topicDelay != null) {
            for(Map.Entry<String, Long> entry : topicDelay.entrySet()) {
                topicDelay.put(entry.getKey(), 0L);
            }
        }
        if(topicRows != null) {
            for(Map.Entry<String, Long> entry : topicRows.entrySet()) {
                topicRows.put(entry.getKey(), 0L);
            }
        }
    }

    public JrdwMonitorVo toJrdwMonitor(int id, String jobId) {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        jmv.setJrdw_mark(jobId);
        //pack the name/value to map
        Map<String, Long> content = new HashMap<String, Long>();
        switch (id) {
            case JDMysqlParserMonitor.FETCH_MONITOR:
                content.put(JDMysqlParserMonitor.FETCH_NUM, fetchNum);
                content.put(JDMysqlParserMonitor.FETCH_SIZE, batchSize);
                content.put(JDMysqlParserMonitor.DELAY_NUM, (lastOffset - nowOffset));
                break;
            case JDMysqlParserMonitor.PERSIS_MONITOR:
                content.put(JDMysqlParserMonitor.SEND_NUM, persisNum);
                content.put(JDMysqlParserMonitor.SEND_SIZE, batchSize);
                content.put(JDMysqlParserMonitor.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlParserMonitor.DELAY_TIME, delayTime);
                break;
            case JDMysqlParserMonitor.TOPIC_DELAY:
                content.putAll(topicDelay);
                break;
            case JDMysqlParserMonitor.TOPIC_ROWS:
                content.putAll(topicRows);
                break;
        }
        //map to json
        JSONObject jo = JSONConvert.MapToJson(content);
        jmv.setContent(jo.toString());
        return jmv;
    }

    public JrdwMonitorVo toJrdwMonitorOnline(int id, String jobId) {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        jmv.setJrdw_mark(jobId);
        //pack the name/value to map
        Map<String, Long> content = new HashMap<String, Long>();
        Map<String, String> msgContent = new HashMap<String, String>();
        Map<String, String> IPContent = new HashMap<String, String>();
        JSONObject jo;
        switch (id) {
            case JDMysqlParserMonitorType.FETCH_MONITOR:
                content.put(JDMysqlParserMonitorType.FETCH_NUM, fetchNum);
                content.put(JDMysqlParserMonitorType.FETCH_SIZE, batchSize);
                content.put(JDMysqlParserMonitorType.DELAY_NUM, (lastOffset - nowOffset));
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlParserMonitorType.PERSIS_MONITOR:
                content.put(JDMysqlParserMonitorType.SEND_NUM, persisNum);
                content.put(JDMysqlParserMonitorType.SEND_SIZE, batchSize);
                content.put(JDMysqlParserMonitorType.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlParserMonitorType.DELAY_TIME, delayTime);
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlParserMonitorType.TOPIC_DELAY:
                content.putAll(topicDelay);
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlParserMonitorType.TOPIC_ROWS:
                content.putAll(topicRows);
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlParserMonitorType.EXCEPTION_MONITOR:
                msgContent.put(JDMysqlParserMonitorType.EXCEPTION, exMsg);
                jo = JSONConvert.MapToJson(msgContent);
                break;
            case JDMysqlParserMonitorType.IP_MONITOR:
                IPContent.put(JDMysqlParserMonitorType.IP, ip);
                jo = JSONConvert.MapToJson(IPContent);
                break;
            case JDMysqlParserMonitorType.TOPIC_MONITOR:
                content.putAll(topicRows);
                jo = JSONConvert.MapToJson(content);
                break;
            default:
                jo = new JSONObject();
                break;
        }
        //map to json
        jmv.setContent(jo.toString());
        return jmv;
    }

}
