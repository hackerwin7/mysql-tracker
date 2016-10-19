package com.github.hackerwin7.mysql.tracker.protocol.json;

import com.github.hackerwin7.mysql.tracker.monitor.JrdwMonitorVo;
import net.sf.json.JSONObject;

import java.util.Map;

/**
 * Created by hp on 15-1-8.
 */
public class JSONConvert {

    public static JSONObject MapToJson(Map m) throws Exception {
        if(m == null) return null;
        return JSONObject.fromObject(m);
    }

    public static JSONObject JrdwMonitorVoToJson(JrdwMonitorVo jmv) throws Exception {
        if(jmv == null) return null;
        return JSONObject.fromObject(jmv);
    }
}
