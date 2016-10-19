import java.io.IOException;

/**
 * Created by hp on 14-9-4.
 */
public class ByteStringTest {

    public static void main(String []args) throws IOException{
        //byte switch to string so not completely correct
        String str = "123";
        byte[] bytes = new byte[]{127,-100,-90,125};
        str = new String(bytes,"UTF-8");
        byte[] bytes1 = str.getBytes("UTF-8");
    }

}
