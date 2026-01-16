import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestRoutines_takeAfter4_negative {

    private Routines routines;
    private int[] paramArray;

    public TestRoutines_takeAfter4_negative(int[] paramArray) {
        this.paramArray = paramArray;
    }

    @Before
    public void before() {
        routines = new Routines();
    }

    @After
    public void after() {
        routines = null;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {new int[] {}},
            {new int[] {1}},
            {new int[] {2}},
            {new int[] {3}},
            {new int[] {5}},
            {new int[] {6}},
            {new int[] {7}},
            {new int[] {8}},
            {new int[] {9}},
            {new int[] {0}},
            {new int[] {1,2,3,5,6,7,8,9,0}},
            {new int[] {0,9,8,7,6,5,3,2,1}},
        });
    }

    @Test(expected = RuntimeException.class)
    public void testTakeAfter4() {
        routines.takeAfter4(paramArray);
    }
}