package com.github.hackerwin7.mysql.tracker.tracker.common;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

import java.util.Map;

/**
 * Created by hp on 14-9-3.
 */
public class PatternUtils {

    private static Map<String, Pattern> patterns = new MapMaker().softValues().makeComputingMap(
            new Function<String, Pattern>() {

                public Pattern apply(
                        String pattern) {
                    try {
                        PatternCompiler pc = new Perl5Compiler();
                        return pc.compile(
                                pattern,
                                Perl5Compiler.CASE_INSENSITIVE_MASK
                                        | Perl5Compiler.READ_ONLY_MASK
                                        | Perl5Compiler.SINGLELINE_MASK);
                    } catch (MalformedPatternException e) {
                        throw new NullPointerException("PatternUtils error!");
                    }
                }
            });

    public static Pattern getPattern(String pattern) {
        return patterns.get(pattern);
    }

    public static void clear() {
        patterns.clear();
    }

}
