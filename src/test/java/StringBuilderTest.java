/**
 * Created by hp on 6/9/15.
 */
public class StringBuilderTest {
    public static void main(String[] args) throws Exception {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i <= 9 ; i++) {
            sb.append(i);
            sb.append("#");
        }
        System.out.println(sb.toString());
        System.out.println(sb.length());
        sb = sb.deleteCharAt(sb.length() - 1);
        System.out.println(sb.toString());
        System.out.println(sb.length());
    }
}
