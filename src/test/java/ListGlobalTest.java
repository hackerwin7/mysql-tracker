import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-12-16.
 */
public class ListGlobalTest {

    public static void main(String[] args) throws Exception {
        ListGlobalTest test = new ListGlobalTest();
        test.run();
    }

    public void run() throws Exception {
        List<Hdata> slist = new ArrayList<Hdata>();
        String str = "1:2:3:4";
        String ss[] = str.split(":");
        Hdata testhh = null;
        for(String s : ss) {
            Hdata hh = S2H(s);
            testhh = hh;
            slist.add(hh);
            System.out.println("loop:" + slist.size());
        }
        System.out.println("size:" + slist.size());
        System.out.print("hh :"+testhh);
    }

    private Hdata S2H(String str) {
        return new Hdata(str.length());
    }

    class Hdata {
        public int kk;
        public Hdata(int s) {
            kk = s;
        }
    }

}
