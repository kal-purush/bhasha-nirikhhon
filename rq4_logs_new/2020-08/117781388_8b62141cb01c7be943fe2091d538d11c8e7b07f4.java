package Math;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;

public class ExcelSheetColumnNumberTest {

    @Mock
    private ExcelSheetColumnNumber solution;

    @Before
    public void setUp() {
        solution = new ExcelSheetColumnNumber();
    }

    @Test
    public void testcase1() {
        String input = "A";
        int expected = 1;
        assertEquals(expected, solution.titleToNumber(input));
    }

    @Test
    public void testcase2() {
        String input = "AB";
        int expected = 28;
        assertEquals(expected, solution.titleToNumber(input));
    }

    @Test
    public void testcase3() {
        String input = "ZY";
        int expected = 701;
        assertEquals(expected, solution.titleToNumber(input));
    }
}