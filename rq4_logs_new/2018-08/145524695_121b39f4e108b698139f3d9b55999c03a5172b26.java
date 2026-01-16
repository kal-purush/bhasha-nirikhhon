
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * SumR Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>八月 21, 2018</pre>
 */
public class SumRTest {

    @Before
    public void before() throws Exception {
        System.out.println("before");
    }

    @After
    public void after() throws Exception {
        System.out.println("after");
    }

    /**
     * Method: sum(int R)
     */
    @Test
    public void testSum() throws Exception {
        Assert.assertEquals(6, new SumR().sum(3));
    }

} 