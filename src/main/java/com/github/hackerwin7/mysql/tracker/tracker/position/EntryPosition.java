package com.github.hackerwin7.mysql.tracker.tracker.position;

//import org.apache.hadoop.hbase.avro.generated.HBase;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.CanalEntry;
import com.github.hackerwin7.mysql.tracker.tracker.MysqlTracker;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-9-2.
 */
public class EntryPosition {


    public String address;

    public int port;

    public String username;

    public String password;

    //test slaveId
    private long slaveId = 987901;

    //当前位点offset 所在的 binlog 文件名
    private String journalName ;

    //当前binlog文件的 具体位点的offset量
    private Long position;

    //位点信息的存储，文件位置
    private String binlogPosFileName = "BinlogPosition.dat";

    //wirte the entryBytesCode file wirte position(from start position write),we save the line number as the position in this file temporarily
    private long entryBytesWriteLine = 0;

    //save the Entry bytes file's write position(for tracker use),now we save the line number as the position in this file temporarily
    private String entryBytesWritePosFileName = "EntryBytesWritePosition.dat";

    //read the entryBytesCode file code position(from start position read)
    private long entryBytesReadLine = 0;

    //save the entry bytes file's read position(for parser use)
    private String entryBytesReadPosFileName = "EntryBytesReadPosition.dat";

    //get the entryBytesCodeFileName's part of data
    private List<byte []> entryByteList = new ArrayList<byte[]>();

    //the byte[] switch to Entry
    private List<CanalEntry.Entry> entryList = new ArrayList<CanalEntry.Entry>();

    //save the entries bytes code to file
    private String entryBytesCodeFileName = "EntryBytesCode.dat";

    //per minute set the binlog position
    private String minuteBinlogFileName = "MinuteBinlogPosition.dat";

    //HBase Operator Corresponding the File Operator

    public EntryPosition() {
        journalName = null;
        position = null;
    }

    public EntryPosition(String journalName, Long position) {
        this.journalName = journalName;
        this.position = position;
    }

    public EntryPosition(String journalName, Long position, String ads, int pot, String usr, String pas ) {
        this.journalName = journalName;
        this.position = position;
        this.address = ads;
        this.port = pot;
        this.username = usr;
        this.password = pas;
    }

    public String getMinuteBinlogFileName() {
        return minuteBinlogFileName;
    }

    public String getBinlogPosFileName() {
        return journalName;
    }

    public String getEntryBytesWritePosFileName() {
        return entryBytesWritePosFileName;
    }

    public String getEntryBytesReadPosFileName() {
        return entryBytesReadPosFileName;
    }

    public String getEntryBytesCodeFileName() {
        return entryBytesCodeFileName;
    }

    public List<CanalEntry.Entry> getEntryList() {
        return entryList;
    }

    public void setEntryList(List<CanalEntry.Entry> entryList) {
        this.entryList = entryList;
    }

    public List<byte[]> getEntryByteList() {
        return entryByteList;
    }

    public void setEntryByteList(List<byte[]> entryByteList) {
        this.entryByteList = entryByteList;
    }

    public long getEntryBytesReadLine() {
        return entryBytesReadLine;
    }

    public void setEntryBytesReadLine(long entryBytesReadLine) {
        this.entryBytesReadLine = entryBytesReadLine;
    }

    public long getEntryBytesWriteLine() {
        return entryBytesWriteLine;
    }

    public void setEntryBytesWriteLine(long entryBytesWriteLine) {
        this.entryBytesWriteLine = entryBytesWriteLine;
    }

    public String getJournalName() {
        return journalName;
    }

    public Long getPosition() {
        return position;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public void setPosition(Long position) {
        this.position = position;
    }


    //位点信息IO
    //读取位点信息，并加载在内存中
    public boolean readBinlogPosFile() throws IOException {
        File datFile = new File(this.binlogPosFileName);
        if(!datFile.exists()||datFile.isDirectory()){
            return(false);
        }
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String dataString = br.readLine();
        String [] datSplit = dataString.split(":");
        this.journalName = datSplit[0];
        this.position = Long.valueOf(datSplit[1]);
        br.close();
        return(true);
    }

    //从内存中的位点信息 写到磁盘
    public void writeBinlogPosFile() throws IOException{
        if(this.journalName!=null&&this.position!=0) {
            File datFile = new File(this.binlogPosFileName);
            if (!datFile.exists()) {
                datFile.createNewFile();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(datFile));
            String dataString = this.journalName+":"+this.position;
            bw.write(dataString);
            bw.newLine();
            bw.flush();
            bw.close();
        }
    }

    public boolean readEntryBytesWritePosFile() throws IOException {
        File datFile = new File(entryBytesWritePosFileName);
        if(!datFile.exists()||datFile.isDirectory()){
            return(false);
        }
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String dataString = br.readLine();
        entryBytesWriteLine = Long.valueOf(dataString);
        br.close();
        return(true);
    }

    public void writeEntryBytesWritePosFile() throws IOException {
        File datFile = new File(entryBytesWritePosFileName);
        if (!datFile.exists()) {
            datFile.createNewFile();
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(datFile));
        String dataString = String.valueOf(entryBytesWriteLine);
        bw.write(dataString);
        bw.newLine();
        bw.flush();
        bw.close();
    }

    //update the file in the MysqlParser
    public boolean readEntryBytesReadPosFile() throws IOException {
        File datFile = new File(entryBytesReadPosFileName);
        if(!datFile.exists()||datFile.isDirectory()){
            return(false);
        }
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String dataString = br.readLine();
        entryBytesReadLine = Long.valueOf(dataString);
        br.close();
        return(true);
    }

    public void writeEntryBytesReadPosFile() throws IOException {
        File datFile = new File(entryBytesReadPosFileName);
        if (!datFile.exists()) {
            datFile.createNewFile();
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(datFile));
        String dataString = String.valueOf(entryBytesReadLine);
        bw.write(dataString);
        bw.newLine();
        bw.flush();
        bw.close();
    }

    public boolean readEntryBytesCodeFile() throws IOException {
        File datFile = new File(entryBytesCodeFileName);
        if(!datFile.exists() || datFile.isDirectory()){
            return false;
        }
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String dataString;
        int lineNumber = 0;
        entryByteList.clear();
        while ((dataString = br.readLine()) != null){
            lineNumber++;
            if(lineNumber >= entryBytesReadLine){
                byte[] entryByte = MysqlTracker.getByteArrayFromString(dataString);
                entryByteList.add(entryByte);
            }
        }
        br.close();
        return(true);
    }

    //in the designated line number wirte the multiple line datas
    //we temporarily append data simply
    public void writeEntryBytesCodeFile() throws IOException {
        File datFile = new File(entryBytesCodeFileName);
        if(!datFile.exists()){
            datFile.createNewFile();
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(datFile,true));//append is true
        for(CanalEntry.Entry entry : entryList){
            byte[] entryByte = entry.toByteArray();
            String inString = MysqlTracker.getStringFromByteArray(entryByte);
            bw.append(inString);
            bw.newLine();
        }
        bw.flush();
        bw.close();
    }

    public boolean readMinuteBinlogPosFile() throws IOException {
        return(false);
    }

    //从内存中的位点信息 写到磁盘
    public void writeMinuteBinlogPosFile() throws IOException{
        if(this.journalName!=null&&this.position!=0) {
            File datFile = new File(minuteBinlogFileName);
            if (!datFile.exists()) {
                datFile.createNewFile();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(datFile));
            String dataString = this.journalName+":"+this.position;
            bw.append(dataString);
            bw.newLine();
            bw.flush();
            bw.close();
        }
    }

    public boolean isValidPos() {
        try {
            MysqlConnector connector = new MysqlConnector(new InetSocketAddress(address, port), username, password);
            MysqlUpdateExecutor updateExecutor = new MysqlUpdateExecutor(connector);
            DirectLogFetcherChannel fetcher = new DirectLogFetcherChannel(connector.getReceiveBufferSize());
            updateExecutor.update("set wait_timeout=9999999");
            updateExecutor.update("set net_write_timeout=1800");
            updateExecutor.update("set net_read_timeout=1800");
            updateExecutor.update("set names 'binary'");//this will be my try to test no binary
            updateExecutor.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
            updateExecutor.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
            BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
            binDmpPacket.binlogFileName = journalName;
            binDmpPacket.binlogPosition = position;
            binDmpPacket.slaveServerId = slaveId;
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(connector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            fetcher.start(connector.getChannel());
            fetcher.fetch();
            fetcher.close();
            connector.disconnect();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
