import com.github.hackerwin7.mysql.tracker.filter.FilterMatcher;

/**
 * Created by hp on 14-12-1.
 */
public class RegexTest {

    public static void main(String[] args) {
        //matches()判断字符串是否匹配某个表达式，"."表示任何一个字符
//        print("abc".matches("..."));
//        //将字符串"a2389a"中的数字用*替换，\d 表示“0--9”数字
//        print("a2389a".replaceAll("\\d", "*"));
//        //将任何是a--z的字符串长度为3的字符串进行编译，这样可以加快匹配速度
//        Pattern p = Pattern.compile("[a-z]{3}");
//        //进行匹配，并将匹配结果放在Matcher对象中
//        Matcher m = p.matcher("abc");
//        print(m.matches());
//        //上面的三行代码可以用下面一行代码代替
//        print("abc".matches("[a-z]{3}"));



        print("canal.test".matches("canal\\.test.*"));


        FilterMatcher fm = new FilterMatcher("canal\\..*,test\\..*,.*\\.regex");
        print(fm.isMatch("canal.test"));
        print(fm.isMatch("test.canal"));
        print(fm.isMatch("coco.regex"));
        print(fm.isMatch("coco.tyhyhy"));
        FilterMatcher fms = new FilterMatcher("canal_test\\..*");
        print(fm.isMatch("canal_test.test"));
        String s="canal_test.simple";
        System.out.println(s.matches("canal_test\\.simple"));

    }

    public static void print(Object o){
        System.out.println(o);
    }

}
