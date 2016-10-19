package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/22
 * Time: 10:28 AM
 * Desc: for test env
 */
public class HandlerZkTest {
    public static void main(String[] args) throws Exception {
        HandlerMagpieKafkaCheckpointZk handler = new HandlerMagpieKafkaCheckpointZk();
        Topology topology = new Topology(handler);
        topology.run();
    }
}
