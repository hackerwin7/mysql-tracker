package com.github.hackerwin7.mysql.tracker.mysql.dbsync.event;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogBuffer;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;

/**
 * <pre>
 *   Base class for ignorable log events. Events deriving from
  this class can be safely ignored by slaves that cannot
  recognize them. Newer slaves, will be able to read and
  handle them. This has been designed to be an open-ended
  architecture, so adding new derived events shall not harm
  the old slaves that support ignorable log mysql.dbsync.event mechanism
  (they will just ignore unrecognized ignorable events).

  @note The only thing that makes an mysql.dbsync.event ignorable is that it has
  the LOG_EVENT_IGNORABLE_F flag set.  It is not strictly necessary
  that ignorable mysql.dbsync.event types derive from Ignorable_log_event; they may
  just as well derive from Log_event and pass LOG_EVENT_IGNORABLE_F as
  argument to the Log_event constructor.
  </pre>
 * 
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class IgnorableLogEvent extends LogEvent {

    public IgnorableLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        // do nothing , just ignore log mysql.dbsync.event
    }

}
