package wsffs.springframework.boot.web.servlet.context;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import testApp.TestApplication;
import testApp.TestController;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class AnnotationConfigServletWebServerApplicationContextTest {

    @DisplayName("getBean으로 bean을 조회하면 등록된 bean이 나온다")
    @Test
    void test() {

        // given
        final String basePackage = TestApplication.class.getPackageName();
        final AnnotationConfigServletWebServerApplicationContext sut =
                new AnnotationConfigServletWebServerApplicationContext(basePackage);
        sut.refresh();

        // when
        final TestController testController = sut.getBean("testController", TestController.class);

        // then
        assertNotNull(testController);
    }
}