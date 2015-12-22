package com.jd.bdp.mysql.parser;

import com.jd.bdp.magpie.Topology;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/22
 * Time: 10:48 AM
 * Desc: for zk test env
 */
public class HandlerZkTest {
    public static void main(String[] args) throws Exception {
        HandlerMagpieKafkaCheckpointZk handler = new HandlerMagpieKafkaCheckpointZk();
        Topology topology = new Topology(handler);
        topology.run();
    }
}
