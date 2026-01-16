package by.clevertec.aspectlogger.logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.TestConstructor;
import org.springframework.test.context.TestConstructor.AutowireMode;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@SpringBootTest(classes = LoggingAspect.class)
@RequiredArgsConstructor
@TestConstructor(autowireMode = AutowireMode.ALL)
class LoggingAspectTest {

    private final LoggingAspect loggingAspect;

    @MockBean
    private ProceedingJoinPoint proceedingJoinPoint;

    @MockBean
    private HttpServletRequest request;

    @MockBean
    private ServletRequestAttributes attributes;

    @BeforeEach
    void setUp() {
        RequestContextHolder.setRequestAttributes(attributes);
        when(attributes.getRequest()).thenReturn(request);
    }

    @Test
    public void testLogBefore() {
        when(request.getMethod()).thenReturn(HttpMethod.GET.name());
        when(request.getServletPath()).thenReturn("/path");
        when(proceedingJoinPoint.getSignature()).thenReturn(mock(Signature.class));

        loggingAspect.logBefore(proceedingJoinPoint);

        verify(request, times(1)).getMethod();
        verify(request, times(1)).getServletPath();
        verify(proceedingJoinPoint, times(1)).getSignature();
    }

    @Test
    public void testLogAfter() {
        when(proceedingJoinPoint.getSignature()).thenReturn(mock(Signature.class));

        loggingAspect.logAfter(proceedingJoinPoint, "result");

        verify(proceedingJoinPoint, times(1)).getSignature();
    }
}