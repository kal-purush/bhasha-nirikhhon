import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class HelperTest {

    @Test(timeout = 2000)
    public void checkIsIntNumber() throws Exception {

        boolean isInt = Helper.isInteger("123");
        assertThat(isInt, is(true));
    }

    @Test(timeout = 2000)
    public void checkIsIntNotNumber() throws Exception {

        boolean isInt = Helper.isInteger("12ee3");
        assertThat(isInt, is(false));
    }

}