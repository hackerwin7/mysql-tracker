package com.github.hackerwin7.mysql.tracker.filter;

import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Created by hp on 14-12-1.
 */
public class FilterMatcher {

    private String pattern;

    private static final String SPLIT = ",";

    private static final String PART = "|";

    private static final Comparator<String> COMPARATOR = new StringCmp();

    public FilterMatcher() {
        String pat = ".*\\.*";
        String ss[] = StringUtils.split(pat, SPLIT);
        List<String> sl = Arrays.asList(ss);
        sl = addHeadEnd(sl);
        Collections.sort(sl, COMPARATOR);
        pattern = StringUtils.join(sl, PART);
    }

    public FilterMatcher(String pat) {
        String ss[] = StringUtils.split(pat, SPLIT);
        List<String> sl = Arrays.asList(ss);
        sl = addHeadEnd(sl);
        Collections.sort(sl, COMPARATOR);
        pattern = StringUtils.join(sl, PART);
    }

    public boolean isMatch(String target) {
        return target.matches(pattern);
    }

    private List<String> addHeadEnd(List<String> list) {
        List<String> results = new ArrayList<String>();
        for (String pat : list) {
            StringBuffer sb = new StringBuffer();
            sb.append("^");
            sb.append(pat);
            sb.append("$");
            results.add(sb.toString());
        }
        return results;
    }

    private static class StringCmp implements Comparator<String> {

        public int compare(String str1, String str2) {
            if(str1.length() > str2.length()) {
                return -1;
            } else if (str1.length() < str2.length()) {
                return 1;
            } else  {
                return 0;
            }
        }
    }

}
