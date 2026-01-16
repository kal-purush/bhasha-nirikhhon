package wsffs.springframework.beans.factory.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultBeanNameGeneratorTest {
    @DisplayName("DefaultBeanNameGenerator 테스트")
    @Test
    void test() {

        // given
        final BeanNameGenerator sut = DefaultBeanNameGenerator.INSTANCE;

        // when
        final String result = sut.generateBeanNameFromClass(DefaultBeanNameGenerator.class);

        // then
        assertEquals("defaultBeanNameGenerator", result);
    }
}