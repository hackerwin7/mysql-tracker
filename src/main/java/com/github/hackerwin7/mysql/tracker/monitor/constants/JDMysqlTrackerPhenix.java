package com.github.hackerwin7.mysql.tracker.monitor.constants;

/**
 * Created by hp on 15-1-8.
 */
public class JDMysqlTrackerPhenix {

    public static final int FETCH_MONITOR = 20000;
    public static final int PERSIS_MONITOR = 20001;
    public static final int EXCEPTION_MONITOR = 20002;

    //fetch
    public static final String FETCH_ROWS = "FETCH_ROWS";
    public static final String FETCH_SIZE = "FETCH_SIZE";
    public static final String DELAY_NUM = "DELAY_NUM";

    //persistence
    public static final String SEND_ROWS = "SEND_ROWS";
    public static final String SEND_SIZE = "SEND_SIZE";
    public static final String DELAY_TIME = "DELAY_TIME";
    public static final String SEND_TIME = "SEND_TIME";

    //exception
    public static final String EXCEPTION = "EXCEPTION";

}
