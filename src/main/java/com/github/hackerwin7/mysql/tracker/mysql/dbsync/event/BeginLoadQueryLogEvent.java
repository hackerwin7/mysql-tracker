package com.github.hackerwin7.mysql.tracker.mysql.dbsync.event;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogBuffer;

/**
 * Event for the first block of file to be loaded, its only difference from
 * Append_block mysql.dbsync.event is that this mysql.dbsync.event creates or truncates existing file
 * before writing data.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class BeginLoadQueryLogEvent extends AppendBlockLogEvent
{
    public BeginLoadQueryLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent)
    {
        super(header, buffer, descriptionEvent);
    }
}
