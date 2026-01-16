package com.spartanullnull.otil.global.exception.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.spartanullnull.otil.config.*;
import com.spartanullnull.otil.global.exception.entity.*;
import java.util.stream.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.*;
import org.springframework.test.context.*;
import org.springframework.test.context.junit.jupiter.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {MessageSourceConfig.class, ErrorCaseResolver.class})
class ErrorCaseResolverTest {

    @Autowired
    MessageSource messageSource;
    @Autowired
    ErrorCaseResolver errorCaseResolver;

    public static Stream<Arguments> createErrorCaseOfDataError() {
        return Stream.of(
            Arguments.of(
                ErrorCase.DATA_ERROR
            )
        );
    }

    @Test
    @DisplayName("Post Construct를 통해 static context field에 MessageSource를 주입받습니다.")
    public void MessageSource_정적주입() {
        // THEN
        assertEquals(errorCaseResolver.getWiredMessageSource(), messageSource);
        assertEquals(ErrorCaseResolver.messageSource, messageSource);
    }

    @ParameterizedTest
    @MethodSource("createErrorCaseOfDataError")
    @DisplayName("에러케이스에 대해 에러 코드를 반환합니다.")
    public void 에러케이스_에러코드_반환_해피케이스(ErrorCase errorCase) {
        // WHEN
        int code = ErrorCaseResolver.getCode(errorCase);

        // THEN
        assertEquals(-9002, code);
    }

    @ParameterizedTest
    @MethodSource("createErrorCaseOfDataError")
    @DisplayName("에러케이스에 대해 에러 코드를 반환합니다.")
    public void 에러케이스_에러코드_반환_언해피케이스() {
        // WHEN
        NoSuchMessageException noSuchMessageException = assertThrows(NoSuchMessageException.class,
            () -> ErrorCaseResolver.getMessage("invalidErrorCaseName", null)
        );

        // THEN
        System.out.println(noSuchMessageException.getMessage());
    }
}