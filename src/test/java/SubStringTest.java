/**
 * Created by hp on 15-3-12.
 */
public class SubStringTest {

    public static void main(String[] args) throws Exception {
        String str = new String("mysql-bin.000242");
        for(int i = str.length() - 1; i >= 0; i--) {
            if(!Character.isDigit(str.charAt(i))) {
                String substr = str.substring(i + 1, str.length());
                long num = Long.valueOf(substr);
                System.out.println(num);
                break;
            }
        }
    }

}
