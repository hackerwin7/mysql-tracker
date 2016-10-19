/**
 * Created by hp on 15-3-12.
 */
public class FuncTest1 {

    private long getRearNum(String str) {
        long ret = 0;
        for(int i = str.length() - 1; i >= 0; i--) {
            if(!Character.isDigit(str.charAt(i))) {
                String substr = str.substring(i + 1, str.length());
                ret = Long.valueOf(substr);
                break;
            }
        }
        if(ret == 0) {
            ret = Long.valueOf(str);
        }
        return ret;
    }

    private double getDelayNum(String logfile1, long pos1, String logfile2, long pos2) {
        long filenum1 = getRearNum(logfile1);
        long filenum2 = getRearNum(logfile2);
        long subnum1 = filenum1 - filenum2;
        long subnum2 = pos1 - pos2;
        if(subnum1 != 0) {
            subnum2 = pos1;
        }
        String s = subnum1 + "." +subnum2;
        double ret = Double.valueOf(s);
        return  ret;
    }

    public static void main(String[] args) {
        FuncTest1 fc = new FuncTest1();
        double d = fc.getDelayNum("mysql-bin.0001234", 256986324, "mysql-bin.0001129", 32653998);
        System.out.println(d);
    }
}
