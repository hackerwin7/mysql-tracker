package com.github.hackerwin7.mysql.tracker.deployer;

import org.apache.log4j.Logger;
import com.github.hackerwin7.mysql.tracker.tracker.HandlerKafkaZkLocalPerformance;

/**
 * Created by hp on 15-3-2.
 */
public class LocalTracker {
    private static Logger logger = Logger.getLogger(LocalTracker.class);

    private static boolean running = true;

    public static void main(String[] args) throws Exception {
        while (true) {
            try {
                final HandlerKafkaZkLocalPerformance handler = new HandlerKafkaZkLocalPerformance();
                handler.prepare("mysql-tracker-json");
                while(running) {
                    handler.run();
                }
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try {
                            running = false;
                            handler.close("mysql-tracker-json");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                Thread.sleep(3000);
            }
        }
    }

}
