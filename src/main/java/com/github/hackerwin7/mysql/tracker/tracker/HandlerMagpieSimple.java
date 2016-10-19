package com.github.hackerwin7.mysql.tracker.tracker;

import com.jd.bdp.magpie.MagpieExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hp on 14-12-27.
 */
public class HandlerMagpieSimple implements MagpieExecutor {

    private Logger logger = LoggerFactory.getLogger(HandlerMagpieSimple.class);

    public HandlerMagpieSimple() {

    }

    public void prepare(String id) throws Exception {
        logger.info("preparing......");
    }

    public void pause(String id) throws Exception {

    }

    public void run() throws Exception {
        logger.info("running......");
    }

    public void close(String id) throws Exception {

    }

    public void reload(String id) throws Exception {

    }

}
