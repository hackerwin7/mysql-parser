package com.github.hackerwin7.mysql.parser.monitor.constants;

/**
 * Created by hp on 15-1-15.
 */
public class JDMysqlParserMonitorType {

    public static final int JD_MYSQL_PARSER_MONITOR_TYPE_MIN = 22000;
    public static final int FETCH_MONITOR = 22001;
    public static final int PERSIS_MONITOR = 22002;
    public static final int TOPIC_DELAY = 31104;
    public static final int TOPIC_ROWS = 31102;
    public static final int EXCEPTION_MONITOR = 22003;
    public static final int IP_MONITOR = 22004;
    public static final int TOPIC_MONITOR = 22005;
    public static final int JD_MYSQL_PARSER_MONITOR_TYPE_MAX = 22999;

    // fetch
    public static final String FETCH_NUM = "FETCH_NUM";
    public static final String FETCH_SIZE = "FETCH_SIZE";
    public static final String DELAY_NUM = "DELAY_NUM";

    // persistence
    public static final String SEND_NUM = "SEND_NUM";
    public static final String SEND_SIZE = "SEND_SIZE";
    public static final String DELAY_TIME = "DELAY_TIME";
    public static final String SEND_TIME = "SEND_TIME";

    //exception
    public static final String EXCEPTION = "EXCEPTION";

    //ip
    public static final String IP = "IP";
}
