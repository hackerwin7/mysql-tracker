import org.apache.commons.lang.StringUtils;

/**
 * Created by hp on 6/9/15.
 */
public class StringUtilsTest {
    public static void main(String[] args) throws Exception {
        if(StringUtils.containsIgnoreCase("SERVER_Id", "master_SERVER_Id")) {
            System.out.println("contain");
        } else {
            System.out.println("not contain");
        }
    }
}
