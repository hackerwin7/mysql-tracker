import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2016/01/28
 * Time: 5:03 PM
 * Desc:
 */
public class CatchTest {

    private static Logger logger = Logger.getLogger(CatchTest.class);

    public static void main(String[] args) throws Exception {
        String command = "RUN";
        try {
            whiles:
            while (true) {
                switch (command) {
                    case "RELOAD":
                        break;
                    case "RUN":
                        int kk = 1;
                        try {
                            throw new NoSuchMethodError("ddd");
                        } catch (Exception e) {
                            logger.error("error accurs in running process");
                            logger.error(e.getMessage());
                            logger.error(ExceptionUtils.getFullStackTrace(e));
                            throw new RuntimeException(e);
                        } finally {
                            logger.info("end running");
                        }
                    case "INIT":
                        break;
                    case "PAUSE":
                        Thread.sleep(1000);
                        break;
                    case "WAIT":
                        logger.warn("I got a wait command, and I'll exit!");
                        break whiles;
                    case "KILL":
                        logger.warn("I got a kill command, and I'll exit!");
                        break whiles;
                    default:
                        break;
                }
                Thread.sleep(5000);
            }
        } catch (Throwable e) {
            logger.error(ExceptionUtils.getFullStackTrace(e));
            logger.error("error = " + e.getMessage(), e);
        } finally {
            logger.info("This magpie app will be closed");
            System.exit(0);
        }
    }
}
