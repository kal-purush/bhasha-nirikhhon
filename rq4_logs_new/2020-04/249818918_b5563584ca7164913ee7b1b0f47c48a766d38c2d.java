import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DemoTest {
    @Test
    public void demoTest() {
        boolean isDemo = true;
        assertThat(isDemo, is(true));
    }
}