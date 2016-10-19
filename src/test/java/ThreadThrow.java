/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/03
 * Time: 10:45 AM
 * Desc:
 */
public class ThreadThrow {
    public static void main(String[] args) throws Throwable {
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(2000);
                        System.out.println("thread alive...");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        th.start();
//        while (true) {
//            System.out.println("main process alive......");
//            throw new Throwable("error");
//        }
        System.out.println("ending......");
        Thread.sleep(5000);
        System.exit(0);
    }
}
