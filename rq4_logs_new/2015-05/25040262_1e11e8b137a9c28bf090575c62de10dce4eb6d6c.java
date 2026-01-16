public class IntToText {

    private static final String[] SCALES = {"", "thousand", "million", "billion", "trillion", "quadrillion", "quintillion", "sextillion"};
    private static final String[] SUBTWENTY = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
                                               "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"};
    private static final String[] DECADES = {"zero", "ten", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"};

    /**
     * Convert any value from 0 to 999 inclusive, to a string.
     * @param value The value to convert.
     * @param and whether to use the word 'and' in the output.
     * @return a String representation of the value.
     */
    private static final String tripleAsText(int value, boolean and) {
        if (value < 0 || value >= 1000) {
            throw new IllegalArgumentException("Illegal triple-value " + value);
        }

        if (value < SUBTWENTY.length) {
            return SUBTWENTY[value];
        }

        int subhun = value % 100;
        int hun = value / 100;
        StringBuilder sb = new StringBuilder(50);
        if (hun > 0) {
            sb.append(SUBTWENTY[hun]).append(" hundred");
        }
        if (subhun > 0) {
            if (hun > 0) {
                sb.append(and ? " and " : " ");
            }
            if (subhun < SUBTWENTY.length) {
                sb.append(SUBTWENTY[subhun]);
            } else {
                int tens = subhun / 10;
                int units = subhun % 10;
                if (tens > 0) {
                    sb.append(DECADES[tens]);
                }
                if (units > 0) {
                    sb.append(" ").append(SUBTWENTY[units]);
                }
            }
        }

        return sb.toString();
    }

    /**
     * Convert any long input value to a text representation 
     * @param value The value to convert
     * @param useand true if you want to use the word 'and' in the text (eleven thousand and thirteen)
     * @param negname
     * @return
     */
    public static final String asText(long value, boolean useand, String negname) {
        if (value == 0) {
            return SUBTWENTY[0];
        }

        // break the value down in to sets of three digits (thousands).
        int[] thous = new int[SCALES.length];
        boolean neg = value < 0;
        // do not make negative numbers positive, to handle Long.MIN_VALUE
        int scale = 0;
        while (value != 0) {
            // use abs to convert thousand-groups to positive, if needed.
            thous[scale] = Math.abs((int)(value % 1000));
            value /= 1000;
            scale++;
        }

        StringBuilder sb = new StringBuilder(scale * 40);
        if (neg) {
            sb.append(negname).append(" ");
        }
        boolean first = true;
        while (--scale > 0) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            if (thous[scale] > 0) {
                sb.append(tripleAsText(thous[scale], useand)).append(" ").append(SCALES[scale]);
            }

        }

        if (!first && useand && thous[0] != 0) {
            sb.append(" and ");
        }
        sb.append(tripleAsText(thous[0], useand));

        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.printf("%15d %s%n", Integer.MIN_VALUE, asText(Integer.MIN_VALUE, true, "minus"));
        System.out.printf("%15d %s%n", Integer.MAX_VALUE, asText(Integer.MAX_VALUE, true, "minus"));
        System.out.printf("%15d %s%n", 0, asText(0, true, "minus"));
        System.out.printf("%15d %s%n", Long.MIN_VALUE, asText(Long.MIN_VALUE, true, "minus"));
        System.out.printf("%15d %s%n", Long.MAX_VALUE, asText(Long.MAX_VALUE, true, "minus"));
    }
}