package com.github.hackerwin7.mysql.tracker.protocol.json;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.util.JSONTokener;
import org.ho.yaml.Yaml;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.MalformedInputException;
import java.util.HashMap;

/**
 * Created by hp on 14-11-13.
 */
public class ConfigJson {

    private String jsonStr;
    private String urlStr;
    private String loadFile = "input_config.yaml";
    private String jobId;
    private String key = "magpie.address";

    public ConfigJson(String id) {
        jobId = id;
    }

    public ConfigJson(String id, String getKey) {
        jobId = id;
        key = getKey;
    }

    private void getFile() {
        //get the urlStr
        try {
            HashMap ml = Yaml.loadType(this.getClass().getClassLoader().getResource(loadFile).openStream(), HashMap.class);
            urlStr = ml.get(key) + jobId;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getJsonStr() {
        jsonStr = null;
        StringBuffer sb = new StringBuffer();
        try {
            URL url = new URL(urlStr);
            InputStreamReader isr = new InputStreamReader(url.openStream());
            char[] buffer = new char[1024];
            int len = 0;
            while ((len = isr.read(buffer)) != -1) {
                sb.append(buffer,0,len);
            }
        } catch (MalformedInputException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        jsonStr = sb.toString();
    }

    public JSONObject getJson() {
        getFile();
        getJsonStr();
        JSONTokener jsonParser = new JSONTokener(jsonStr);
        JSONObject jsonOb = (JSONObject)jsonParser.nextValue();
        return jsonOb;
    }

    public JSONArray getJsonArr() {
        getFile();
        getJsonStr();
        JSONTokener jsonParser = new JSONTokener(jsonStr);
        System.out.println("json string : " + jsonStr);
        JSONArray ja = (JSONArray) jsonParser.nextValue();
        return ja;
    }

}
