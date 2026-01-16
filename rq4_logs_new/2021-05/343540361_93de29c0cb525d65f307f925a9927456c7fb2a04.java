package problem890;

import java.util.LinkedList;
import java.util.List;

public final class FindAndReplacePattern {

    private static final int LETTERS_CNT = 'z' - 'a' + 1;

    public List<String> findAndReplacePattern(final String[] words, final String pattern) {
        final int commonLen = pattern.length();
        final byte[] patternEncoded = new byte[commonLen];
        final byte[] patternCharToCodeMap = new byte[LETTERS_CNT];

        byte next = 1;
        for (int i = 0; i < commonLen; i++) {
            final int idx = pattern.charAt(i) - 'a';

            if (patternCharToCodeMap[idx] != 0) {
                patternEncoded[i] = patternCharToCodeMap[idx];
            } else {
                patternCharToCodeMap[idx] = next;
                patternEncoded[i] = next++;
            }
        }

        final List<String> result = new LinkedList<>();

        wLoop: for (final String w : words) {
            next = 1;
            final byte[] wCharToCodeMap = new byte[LETTERS_CNT];

            for (int i = 0; i < commonLen; i++) {
                final int idx = w.charAt(i) - 'a';

                if (wCharToCodeMap[idx] != 0) {
                    if (patternEncoded[i] != wCharToCodeMap[idx])
                        continue wLoop;
                } else {
                    if (patternEncoded[i] != next)
                        continue wLoop;
                    wCharToCodeMap[idx] = next++;
                }
            }

            result.add(w);
        }

        return result;
    }

}