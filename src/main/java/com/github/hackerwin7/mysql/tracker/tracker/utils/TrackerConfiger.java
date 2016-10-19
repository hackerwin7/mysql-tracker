package com.github.hackerwin7.mysql.tracker.tracker.utils;

/**
 * Created by hp on 14-9-2.
 */
public class TrackerConfiger {


    private String username ;

    private String password ;

    private String address;

    private int port;

    private Long slaveId;

    private String hbaseRootDir;

    private String hbaseDistributed;

    private String hbaseZkQuorum;

    private String hbaseZkPort;

    private String dfsSocketTimeout;

    private String filterRegex = ".*\\..*";


    public TrackerConfiger() {

    }

    public TrackerConfiger(String username, String password, String address, int port, Long slaveId) {
        this.username = username;
        this.password = password;
        this.address = address;
        this.port = port;
        this.slaveId = slaveId;
    }

    public TrackerConfiger(String username, String password, String address, int port, Long slaveId, String hbaseString) {
        this.username = username;
        this.password = password;
        this.address = address;
        this.port = port;
        this.slaveId = slaveId;
        this.hbaseRootDir = hbaseString;
    }

    public TrackerConfiger(String username, String password, String address, int port, Long slaveId, String hbaseString, String regex) {
        this.username = username;
        this.password = password;
        this.address = address;
        this.port = port;
        this.slaveId = slaveId;
        this.hbaseRootDir = hbaseString;
        this.filterRegex = regex;
    }

    public String getFilterRegex() {
        return filterRegex;
    }

    public void setFilterRegex(String filterRegex) {
        this.filterRegex = filterRegex;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public Long getSlaveId() {
        return slaveId;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    public String getHbaseRootDir() {
        return hbaseRootDir;
    }

    public void setHbaseRootDir(String hbaseRootDir) {
        this.hbaseRootDir = hbaseRootDir;
    }

    public String getHbaseDistributed() {
        return hbaseDistributed;
    }

    public void setHbaseDistributed(String hbaseDistributed) {
        this.hbaseDistributed = hbaseDistributed;
    }

    public String getHbaseZkQuorum() {
        return hbaseZkQuorum;
    }

    public void setHbaseZkQuorum(String hbaseZkQuorum) {
        this.hbaseZkQuorum = hbaseZkQuorum;
    }

    public String getHbaseZkPort() {
        return hbaseZkPort;
    }

    public void setHbaseZkPort(String hbaseZkPort) {
        this.hbaseZkPort = hbaseZkPort;
    }

    public String getDfsSocketTimeout() {
        return dfsSocketTimeout;
    }

    public void setDfsSocketTimeout(String dfsSocketTimeout) {
        this.dfsSocketTimeout = dfsSocketTimeout;
    }

}
