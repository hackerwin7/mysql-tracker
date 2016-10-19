package com.github.hackerwin7.mysql.tracker.monitor;

/**
 * Created by hp on 15-1-8.
 */
public class JrdwMonitorVo {

    private int id;
    private String jrdw_mark;
    private String content;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getJrdw_mark() {
        return jrdw_mark;
    }

    public void setJrdw_mark(String jrdw_mark) {
        this.jrdw_mark = jrdw_mark;
    }
}
