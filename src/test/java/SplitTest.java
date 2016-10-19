/**
 * Created by hp on 14-12-25.
 */
public class SplitTest {

    public static void main(String[] args) throws Exception {
        String str = "127.0.0.1";
        String[] ss = str.split(",");
        for(String s : ss) {
            System.out.println(s);
        }

        String str1 = "&ak47";
        String[] ss1 = str1.split("&");
        for(String s : ss1) {
            System.out.println("s = " + s);
        }
    }

}
