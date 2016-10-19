/**
 * Created by hp on 14-12-25.
 */
public class SimpleTest {

    public static void main(String[] args) {
//        Map<String, String> dic = new HashMap<String, String>();
//        dic.put("1", "one");
//        dic.put("2", "two");
//        String ss = dic.get("24");
//        if(ss == null) System.out.println("object null");
//        else if(ss.equals("")) System.out.println("string null \"\"");
//        else System.out.println("not both null , " + ss);

        String ss = "a/u0001b/u0001c/u0001d/u0001";
        System.out.println(ss.lastIndexOf("/u0001"));
        String sa = ss.substring(0, ss.lastIndexOf("/u0001"));
        System.out.println(sa);
    }
}
