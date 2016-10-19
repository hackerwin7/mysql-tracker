/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/03
 * Time: 10:38 AM
 * Desc:
 */
public class ThrowWhile {
    public static void main(String[] args){
        while (true) {
            System.out.println("hello");
            //Thread.sleep(2000);
            throw new RuntimeException("ssd");
        }
    }
}
