package bit;

import bit.dday.domain.Dday;
import com.navercorp.fixturemonkey.FixtureMonkey;
import com.navercorp.fixturemonkey.api.introspector.BuilderArbitraryIntrospector;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FixtureMonkeyTest {

    @Test
    void test() {
        FixtureMonkey monkey = FixtureMonkey.builder()
                .objectIntrospector(BuilderArbitraryIntrospector.INSTANCE)
                .build();

        Dday dday = monkey.giveMeOne(Dday.class);

        Assertions.assertNotNull(dday);
    }
}