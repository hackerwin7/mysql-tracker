import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hp on 14-10-8.
 */
public class PropertiesTest {

    public static void main(String []args) {
        try {
            InputStream in = new BufferedInputStream(new FileInputStream("conf/tracker.properties"));
            Properties pro = new Properties();
            pro.load(in);
            System.out.print(pro.getProperty("hbase.rootdir"));
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
