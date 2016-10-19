package com.github.hackerwin7.mysql.tracker.mysql.dbsync.event;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogBuffer;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;

/**
 * 
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class PreviousGtidsLogEvent  extends LogEvent{

    public PreviousGtidsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
        // do nothing , just for mysql gtid search function
    }
}

