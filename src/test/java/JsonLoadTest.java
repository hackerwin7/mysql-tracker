import net.sf.json.JSONObject;
import com.github.hackerwin7.mysql.tracker.protocol.json.ConfigJson;

/**
 * Created by hp on 14-11-13.
 */
public class JsonLoadTest {

    public static void main(String[] args) {
//        String jsonStr= "{\"code\":0,\"info\":{\"job-id\":\"jd-mysql-tracker\",\"content\":{\"Username\":\"jd_data\",\"Address\":\"172.17.36.48\",\"HbaseDistributed\":\"true\",\"HbaseZkQuorum\":\"BJ-YZH-1-H1-3660.jd.com,BJ-YZH-1-H1-3661.jd.com,BJ-YZH-1-H1-3662.jd.com\",\"Password\":\"jd_data\",\"HbaseZkPort\":\"2181\",\"HbaseRootDir\":\"hdfs://BJ-YZH-1-H1-3650.jd.com:9000/hbase\",\"SlaveId\":2234,\"Port\":3306,\"DfsSocketTimeout\":\"180000\"},\"update-time\":\"2014-10-30 13:52:15\",\"type\":\"MYSQL_TRACKER\",\"create-time\":\"2014-10-30 13:52:15\",\"yn\":\"yes\"}}";
//        JSONTokener jsonParser = new JSONTokener(jsonStr);
//        JSONObject jsOb = (JSONObject)jsonParser.nextValue();
//        JSONObject jCont = jsOb.getJSONObject("info").getJSONObject("content");
//        TrackerConfiger configer = new TrackerConfiger();
//        configer.setUsername(jCont.getString("Username"));
//        configer.setPassword(jCont.getString("Password"));
//        configer.setAddress(jCont.getString("Address"));
//        configer.setPort(jCont.getInt("Port"));
//        configer.setSlaveId(jCont.getLong("SlaveId"));
//        configer.setHbaseRootDir(jCont.getString("HbaseRootDir"));
//        configer.setHbaseDistributed(jCont.getString("HbaseDistributed"));
//        configer.setHbaseZkQuorum(jCont.getString("HbaseZkQuorum"));
//        configer.setHbaseZkPort(jCont.getString("HbaseZkPort"));
//        configer.setDfsSocketTimeout(jCont.getString("DfsSocketTimeout"));
//        System.out.print(configer.getUsername()+","+configer.getPassword()+","+configer.getAddress()+"," +
//                configer.getPort()+","+configer.getSlaveId()+","+configer.getHbaseRootDir()+"," +
//                configer.getHbaseDistributed()+","+configer.getHbaseZkQuorum()+","+configer.getHbaseZkPort()+"," +
//                configer.getDfsSocketTimeout());
        ConfigJson jcnf = new ConfigJson("", "online.address");
        JSONObject root = jcnf.getJson();
    }

}
