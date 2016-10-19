package com.github.hackerwin7.mysql.tracker.tracker.common;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.apache.commons.lang.StringUtils;


import java.util.*;

/**
 * Created by hp on 14-9-3.
 */
public class AviaterRegexFilter {

    private static final String             SPLIT             = ",";
    private static final String             PATTERN_SPLIT     = "|";
    private static final String             FILTER_EXPRESSION = "regex(pattern,target)";
    private static final RegexFunction regexFunction     = new RegexFunction();
    private final Expression exp               = AviatorEvaluator.compile(FILTER_EXPRESSION, true);
    static {
        AviatorEvaluator.addFunction(regexFunction);
    }

    private static final Comparator<String> COMPARATOR        = new StringComparator();

    final private String                    pattern;

    public AviaterRegexFilter(String pattern){
        List<String> list = null;
        if (StringUtils.isEmpty(pattern)) {
            list = new ArrayList<String>();
        } else {
            String[] ss = StringUtils.split(pattern, SPLIT);
            list = Arrays.asList(ss);
        }

        // 对pattern按照从长到短的排序
        // 因为 foo|foot 匹配 foot 会出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot 的长度不一样
        Collections.sort(list, COMPARATOR);
        // 对pattern进行头尾完全匹配
        list = completionPattern(list);
        this.pattern = StringUtils.join(list, PATTERN_SPLIT);
    }

    public boolean filter(String filtered) throws CanalParseException {
        if (StringUtils.isEmpty(pattern)) {
            return true;
        }

        if (StringUtils.isEmpty(filtered)) {
            return true;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put("pattern", pattern);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

    /**
     * 修复正则表达式匹配的问题，因为使用了 oro 的 matches，会出现：
     *
     * <pre>
     * foo|foot 匹配 foot 出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot 的长度不一样
     * </pre>
     *
     * 因此此类对正则表达式进行了从长到短的排序
     *
     * @author zebin.xuzb 2012-10-22 下午2:02:26
     * @version 1.0.0
     */
    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String str1, String str2) {
            if (str1.length() > str2.length()) {
                return -1;
            } else if (str1.length() < str2.length()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * 修复正则表达式匹配的问题，即使按照长度递减排序，还是会出现以下问题：
     *
     * <pre>
     * foooo|f.*t 匹配 fooooot 出错，原因是 fooooot 匹配了 foooo 之后，会将 fooo 和数据进行匹配，但是 foooo 的长度和 fooooot 的长度不一样
     * </pre>
     *
     * 因此此类对正则表达式进行头尾完全匹配
     *
     * @author simon
     * @version 1.0.0
     */

    private List<String> completionPattern(List<String> patterns) {
        List<String> result = new ArrayList<String>();
        for (String pattern : patterns) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("^");
            stringBuffer.append(pattern);
            stringBuffer.append("$");
            result.add(stringBuffer.toString());
        }
        return result;
    }

}
