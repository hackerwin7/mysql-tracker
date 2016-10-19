package com.github.hackerwin7.mysql.tracker.deployer;

import com.github.hackerwin7.mysql.tracker.tracker.SimpleMysqlTracker;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by hp on 15-3-9.
 */
public class SimpleLocalTracker {
    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(file2in("log4j.properties", "tracker.log4j"));
        SimpleMysqlTracker tracker = new SimpleMysqlTracker();
        tracker.start();
    }

    public static InputStream file2in(String filename, String prop) throws Exception {
        String cnf = System.getProperty(prop, "classpath:" + filename);
        InputStream in = null;
        if(cnf.startsWith("classpath:")) {
            cnf = StringUtils.substringAfter(cnf, "classpath:");
            in = SimpleLocalTracker.class.getClassLoader().getResourceAsStream(cnf);
        } else {
            in = new FileInputStream(cnf);
        }
        return in;
    }
}
