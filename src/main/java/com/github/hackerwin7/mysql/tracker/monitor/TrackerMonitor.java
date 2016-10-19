package com.github.hackerwin7.mysql.tracker.monitor;

import com.github.hackerwin7.mysql.tracker.monitor.constants.JDMysqlTrackerMonitorType;
import com.github.hackerwin7.mysql.tracker.monitor.constants.JDMysqlTrackerPhenix;
import net.sf.json.JSONObject;
import com.github.hackerwin7.mysql.tracker.protocol.json.JSONConvert;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 14-9-23.
 */
public class TrackerMonitor implements Cloneable {

    public long fetchStart;

    public long fetchEnd;

    public long persistenceStart;

    public long persistenceEnd;

    public long sendStart;

    public long sendEnd;

    public long perMinStart;

    public long perMinEnd;

    public long hbaseReadStart;

    public long hbaseReadEnd;

    public long hbaseWriteStart;

    public long hbaseWriteEnd;

    public long serializeStart;

    public long serializeEnd;

    public long fetchNum;

    public long persisNum;

    public long batchSize;//bytes for unit

    public long fetcherStart;

    public long fetcherEnd;

    public long decodeStart;

    public long decodeEnd;

    public long delayTime;

    public double delayNum;

    public String exMsg;

    public String ip;

    public TrackerMonitor() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
        delayNum = 0;
        exMsg = ip = "";
    }

    public Object clone() {
        Object o = null;
        try {
            o = (TrackerMonitor) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }

    public TrackerMonitor cloneDeep() {
        return (TrackerMonitor) clone();
    }

    public void clear() {
        fetchStart = fetchEnd = persistenceStart = persistenceEnd = 0;
        perMinStart = perMinEnd = hbaseReadStart = hbaseReadEnd = 0;
        hbaseWriteStart = hbaseWriteEnd = serializeStart = serializeEnd = 0;
        fetchNum = persisNum = batchSize = 0;
        fetcherStart = fetcherEnd = decodeStart = decodeEnd = 0;
        sendStart = sendEnd = 0;
        delayTime = 0;
        exMsg = ip = "";
    }

    public JrdwMonitorVo toJrdwMonitor(int id) throws Exception {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        //pack the member / value  to the Map<String,String> or Map<String,Long>
        Map<String, Long> content = new HashMap<String, Long>();
        switch (id) {
            case JDMysqlTrackerPhenix.FETCH_MONITOR:
                content.put(JDMysqlTrackerPhenix.FETCH_ROWS, fetchNum);
                content.put(JDMysqlTrackerPhenix.FETCH_SIZE, batchSize);
                break;
            case JDMysqlTrackerPhenix.PERSIS_MONITOR:
                content.put(JDMysqlTrackerPhenix.SEND_ROWS, persisNum);
                content.put(JDMysqlTrackerPhenix.SEND_SIZE, batchSize);
                content.put(JDMysqlTrackerPhenix.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlTrackerPhenix.DELAY_TIME, delayTime);
                break;
            default:
                break;
        }
        JSONObject jo = JSONConvert.MapToJson(content);
        jmv.setContent(jo.toString());
        return jmv;
    }

    public JrdwMonitorVo toJrdwMonitor(int id, String jobId) throws Exception {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        jmv.setJrdw_mark(jobId);
        //pack the member / value  to the Map<String,String> or Map<String,Long>
        Map<String, Long> content = new HashMap<String, Long>();
        switch (id) {
            case JDMysqlTrackerPhenix.FETCH_MONITOR:
                content.put(JDMysqlTrackerPhenix.FETCH_ROWS, fetchNum);
                content.put(JDMysqlTrackerPhenix.FETCH_SIZE, batchSize);
                break;
            case JDMysqlTrackerPhenix.PERSIS_MONITOR:
                content.put(JDMysqlTrackerPhenix.SEND_ROWS, persisNum);
                content.put(JDMysqlTrackerPhenix.SEND_SIZE, batchSize);
                content.put(JDMysqlTrackerPhenix.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlTrackerPhenix.DELAY_TIME, delayTime);
                break;
            default:
                break;
        }
        //map to json
        JSONObject jo = JSONConvert.MapToJson(content);
        jmv.setContent(jo.toString());
        return jmv;
    }

    public JrdwMonitorVo toJrdwMonitorOnline(int id, String jobId) throws Exception {
        JrdwMonitorVo jmv = new JrdwMonitorVo();
        jmv.setId(id);
        jmv.setJrdw_mark(jobId);
        //pack the member / value  to the Map<String,String> or Map<String,Long>
        Map<String, Long> content = new HashMap<String, Long>();
        Map<String, String> msgContent = new HashMap<String, String>();
        Map<String, String> IPContent = new HashMap<String, String>();
        Map<String, Object> newContent = new HashMap<String, Object>();
        JSONObject jo;
        switch (id) {
            case JDMysqlTrackerMonitorType.FETCH_MONITOR:
                newContent.put(JDMysqlTrackerMonitorType.FETCH_ROWS, fetchNum);
                newContent.put(JDMysqlTrackerMonitorType.FETCH_SIZE, batchSize);
                newContent.put(JDMysqlTrackerMonitorType.DELAY_NUM, delayNum);
                jo = JSONConvert.MapToJson(newContent);
                break;
            case JDMysqlTrackerMonitorType.PERSIS_MONITOR:
                content.put(JDMysqlTrackerPhenix.SEND_ROWS, persisNum);
                content.put(JDMysqlTrackerPhenix.SEND_SIZE, batchSize);
                content.put(JDMysqlTrackerPhenix.SEND_TIME, (sendEnd - sendStart));
                content.put(JDMysqlTrackerPhenix.DELAY_TIME, delayTime);
                jo = JSONConvert.MapToJson(content);
                break;
            case JDMysqlTrackerMonitorType.EXCEPTION_MONITOR:
                msgContent.put(JDMysqlTrackerMonitorType.EXCEPTION, exMsg);
                jo = JSONConvert.MapToJson(msgContent);
                break;
            case JDMysqlTrackerMonitorType.IP_MONITOR:
                IPContent.put(JDMysqlTrackerMonitorType.IP, ip);
                jo = JSONConvert.MapToJson(IPContent);
                break;
            default:
                jo = new JSONObject();
                break;
        }
        //map to json
        jmv.setContent(jo.toString());
        return jmv;
    }


}
