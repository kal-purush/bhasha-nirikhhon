package wsffs.springframework.context.annotation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import testApp.TestApplication;
import wsffs.springframework.boot.web.servlet.context.AnnotationConfigServletWebServerApplicationContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClassPathBeanDefinitionScannerTest {

    @DisplayName("지정된 패키지를 스캔 후, 등록된 정의의 개수를 비교한다")
    @Test
    void test() {

        // given
        final String basePackage = TestApplication.class.getPackageName();
        final AnnotationConfigServletWebServerApplicationContext ctx =
                new AnnotationConfigServletWebServerApplicationContext(basePackage);
        final ClassPathBeanDefinitionScanner sut = new ClassPathBeanDefinitionScanner(ctx);

        // when
        final int result = sut.scan(basePackage);

        // then
        assertEquals(2, result);
        assertEquals(2, ctx.getBeanDefinitionCount());
    }
}