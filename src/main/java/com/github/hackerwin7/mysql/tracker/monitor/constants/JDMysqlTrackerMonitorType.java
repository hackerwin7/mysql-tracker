package com.github.hackerwin7.mysql.tracker.monitor.constants;

/**
 * Created by hp on 15-1-15.
 */
public class JDMysqlTrackerMonitorType {

    public static final int JD_MYSQL_TRACKER_MONITOR_TYPE_MIN = 20000;
    public static final int FETCH_MONITOR = 20001;
    public static final int PERSIS_MONITOR = 20002;
    public static final int EXCEPTION_MONITOR = 20003;
    public static final int IP_MONITOR = 20004;
    public static final int JD_MYSQL_TRACKER_MONITOR_TYPE_MAX = 20999;

    // fetch
    public static final String FETCH_ROWS = "FETCH_ROWS";
    public static final String FETCH_SIZE = "FETCH_SIZE";
    public static final String DELAY_NUM = "DELAY_NUM";

    // persistence
    public static final String SEND_ROWS = "SEND_ROWS";
    public static final String SEND_SIZE = "SEND_SIZE";
    public static final String DELAY_TIME = "DELAY_TIME";
    public static final String SEND_TIME = "SEND_TIME";

    //exception
    public static final String EXCEPTION = "EXCEPTION";

    //ip
    public static final String IP = "IP";

}
