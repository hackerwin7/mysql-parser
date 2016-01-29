package com.github.hackerwin7.mysql.parser.parser.utils;

/**
 * Created by hp on 14-9-28.
 */
public class ParserConfig {



    private String hbaseRootDir;

    private String hbaseDistributed;

    private String hbaseZkQuorum;

    private String hbaseZkPort;

    private String dfsSocketTimeout;


    public ParserConfig() {

    }


    public String getHbaseRootDir() {
        return hbaseRootDir;
    }

    public void setHbaseRootDir(String hbaseRootDir) {
        this.hbaseRootDir = hbaseRootDir;
    }

    public String getHbaseDistributed() {
        return hbaseDistributed;
    }

    public void setHbaseDistributed(String hbaseDistributed) {
        this.hbaseDistributed = hbaseDistributed;
    }

    public String getHbaseZkQuorum() {
        return hbaseZkQuorum;
    }

    public void setHbaseZkQuorum(String hbaseZkQuorum) {
        this.hbaseZkQuorum = hbaseZkQuorum;
    }

    public String getHbaseZkPort() {
        return hbaseZkPort;
    }

    public void setHbaseZkPort(String hbaseZkPort) {
        this.hbaseZkPort = hbaseZkPort;
    }

    public String getDfsSocketTimeout() {
        return dfsSocketTimeout;
    }

    public void setDfsSocketTimeout(String dfsSocketTimeout) {
        this.dfsSocketTimeout = dfsSocketTimeout;
    }
}
