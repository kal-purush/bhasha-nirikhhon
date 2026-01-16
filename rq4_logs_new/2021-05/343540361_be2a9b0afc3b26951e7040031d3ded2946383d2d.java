package problem816;

import java.util.ArrayList;
import java.util.List;

public final class AmbiguousCoordinates {

    public List<String> ambiguousCoordinates(final String s) {
        if (s.length() == 4)
            return List.of("(" + s.charAt(1) + ", " + s.charAt(2) + ')');

        final StringBuilder sb = new StringBuilder(10);
        sb.append('(');

        final int len = s.length() - 2;
        final char[] ch = new char[len];

        for (int i = 1; i < s.length() - 1; i++)
            ch[i - 1] = s.charAt(i);

        final List<String> results = new ArrayList<>();

        boolean leadingZerosLeft, leadingZerosRight;
        boolean trailingZerosLeft, trailingZerosRight;

        for (int sp = 1; sp <= len - 1; sp++) {
            // originally I wanted to dynamically update zeros counters, but it does not make any sense with so small
            // number of characters

            leadingZerosLeft = ch[0] == '0';
            trailingZerosLeft = ch[sp - 1] == '0';
            leadingZerosRight = ch[sp] == '0';
            trailingZerosRight = ch[len - 1] == '0';

            if ((leadingZerosLeft && trailingZerosLeft && sp > 1) || (leadingZerosRight && trailingZerosRight && sp < len - 1)) {
                // invalid case
                continue;
            } else {
                List<String> leftPos;
                if (leadingZerosLeft) {
                    sb.setLength(0);
                    sb.append('(');
                    sb.append(sp == 1 ? '0' : "0.");
                    for (int i = 1; i < sp; i++)
                        sb.append(ch[i]);
                    sb.append(", ");
                    leftPos = List.of(sb.toString());
                } else {
                    leftPos = new ArrayList<>();
                    for (int i = 0; i < sp; i++) {
                        sb.setLength(0);
                        sb.append('(');
                        if (i > 0) {
                            for (int j = 0; j < i; j++)
                                sb.append(ch[j]);
                            sb.append('.');
                        }
                        for (int j = i; j < sp; j++)
                            sb.append(ch[j]);
                        sb.append(", ");
                        leftPos.add(sb.toString());

                        if (trailingZerosLeft)
                            break;
                    }
                }

                if (leadingZerosRight) {
                    sb.setLength(0);
                    sb.append(sp == len - 1 ? '0' : "0.");
                    for (int i = sp + 1; i < len; i++)
                        sb.append(ch[i]);
                    sb.append(')');

                    for (var l : leftPos)
                        results.add(l + sb);
                } else {
                    for (int i = sp; i < len; i++) {
                        sb.setLength(0);
                        if (i > sp) {
                            for (int j = sp; j < i; j++)
                                sb.append(ch[j]);
                            sb.append('.');
                        }
                        for (int j = i; j < len; j++)
                            sb.append(ch[j]);
                        sb.append(')');

                        for (var l : leftPos)
                            results.add(l + sb);

                        if (trailingZerosRight)
                            break;
                    }
                }
            }
        }

        return results;
    }

}