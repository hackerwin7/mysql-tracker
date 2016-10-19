import com.github.hackerwin7.mysql.tracker.kafka.utils.KafkaConf;

/**
 * Created by hp on 14-12-11.
 */
public class ZkSimpleClient {

    private static String zkServers = "127.0.0.1:2181";

    public static void main(String[] args) throws Exception {
//        ZooKeeper zk = new ZooKeeper(zkServers, 100000, new Watcher() {
//            @Override
//            public void process(WatchedEvent event) {
//                System.out.println("watcher : " + event.getType());
//            }
//        });
//        if(zk.exists("/mysql-tracker",false) == null)
//            zk.create("/mysql_tracker", "mysql-log-tracker".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//
//        zk.create("/mysql_tracker/persistence", "save position".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//
//        zk.setData("/mysql_tracker/persistence", "persistent the position".getBytes(), -1);
//
//        byte[] result = zk.getData("/mysql_tracker/persistence", new Watcher() {
//            @Override
//            public void process(WatchedEvent event) {
//                System.out.println("get data watcher : " + event.getType());
//            }
//        }, null);
//
//        System.out.println("get data result : " + new String(result));
//
//        zk.close();

        String dataKafkaZk = "127.0.0.1:2181" + "/kafka";
        KafkaConf dataCnf = new KafkaConf();
        dataCnf.loadZk(dataKafkaZk);
    }

}
