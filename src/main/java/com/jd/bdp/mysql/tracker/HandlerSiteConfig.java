package com.jd.bdp.mysql.tracker;

import com.jd.bdp.magpie.Topology;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2016/01/28
 * Time: 10:52 AM
 * Desc: magpie for site config interface
 */
public class HandlerSiteConfig {
    public static void main(String[] args) throws Exception {
        HandlerMagpieKafkaCheckpointHBaseSiteConfig handler = new HandlerMagpieKafkaCheckpointHBaseSiteConfig();
        Topology topology = new Topology(handler);
        topology.run();
    }
}
