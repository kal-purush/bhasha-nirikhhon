package seedu.commons;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UtilTest {

    @Test
    void generatePadStringWithCharAndLength_zeroLength_success() {
        assertEquals("", Util.GeneratePadStringWithCharAndLength(' ', 0));
        assertEquals("", Util.GeneratePadStringWithCharAndLength('$', 0));
    }

    @Test
    void generatePadStringWithCharAndLength_nonZeroLength_success() {
        assertEquals("$$$$$$", Util.GeneratePadStringWithCharAndLength('$', 6));
        assertEquals("----", Util.GeneratePadStringWithCharAndLength('-', 4));
    }

    @Test
    void generatePadStringWithCharAndLength_negativeLength_success() {
        // Effect of - will be left adjust, which doesnt affect the return value.
        assertEquals("$", Util.GeneratePadStringWithCharAndLength('$', -1));
        assertEquals("$$", Util.GeneratePadStringWithCharAndLength('$', -2));
    }

    @Test
    void limitStringWithDots_LengthGreaterThanThree_success() {
        assertEquals("abc...", Util.limitStringWithDots("abcdefg", 6));
        assertEquals("abcdef", Util.limitStringWithDots("abcdef", 6));
        assertEquals("abcdefg", Util.limitStringWithDots("abcdefg", 10));
    }

    @Test
    void limitStringWithDots_LengthEqualThree_success() {
        assertEquals("...", Util.limitStringWithDots("abcdefg", 3));
        assertEquals("abc", Util.limitStringWithDots("abc", 3));
    }

    @Test
    void limitStringWithDots_LengthLessThanThree_success() {
        assertEquals("ab", Util.limitStringWithDots("abcdefg", 2));
        assertEquals("", Util.limitStringWithDots("abcdef", -1));
        assertEquals("", Util.limitStringWithDots("abcdefg", 0));
    }
}