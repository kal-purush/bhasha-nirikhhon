package com.example.auth_service.aspect;

import com.example.auth_service.annotation.RateLimit;
import com.example.auth_service.exception.RateLimitExceededException;
import jakarta.servlet.http.HttpServletRequest;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RateLimitAspectTest {

    @Mock
    private RedisTemplate<String, Long> rateLimitRedisTemplate;

    @Mock
    private ValueOperations<String, Long> valueOperations;

    @Mock
    private ProceedingJoinPoint joinPoint;

    @Mock
    private Signature signature;

    @Mock
    private HttpServletRequest request;

    @Mock
    private ServletRequestAttributes requestAttributes;

    @InjectMocks
    private RateLimitAspect rateLimitAspect;

    @BeforeEach
    void setUp() {
        // Настройка моков
        when(rateLimitRedisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    @DisplayName("Должен разрешить запрос, когда количество запросов в пределах лимита")
    void testRateLimitWithinLimit() throws Throwable {
        // Arrange
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(60);
        when(rateLimit.key()).thenReturn("");
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(joinPoint.proceed()).thenReturn("success");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // Act
        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);

        // Assert
        assertEquals("success", result);
        verify(rateLimitRedisTemplate).expire(anyString(), eq(60L), any());
    }

    @Test
    @DisplayName("Должен выбросить исключение при превышении лимита запросов")
    void testRateLimitExceeded() {
        // Arrange
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.key()).thenReturn("");
        when(valueOperations.increment(anyString())).thenReturn(6L);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // Act & Assert
        assertThrows(RateLimitExceededException.class, () -> {
            rateLimitAspect.rateLimit(joinPoint, rateLimit);
        });
    }

    @Test
    @DisplayName("Должен использовать пользовательский ключ, если он указан")
    void testCustomKey() throws Throwable {
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(60);
        when(rateLimit.key()).thenReturn("custom:kay");
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(joinPoint.proceed()).thenReturn("success");

        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);

        assertEquals("success", result);
        verify(valueOperations).increment("custom:kay");
    }

    @Test
    @DisplayName("Должен генерировать корректный ключ на основе метода и IP-адреса")
    void testKeyGenerator() throws Throwable {
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(60);
        when(rateLimit.key()).thenReturn("");
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(joinPoint.proceed()).thenReturn("success");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);
        assertEquals("success", result);
        verify(valueOperations).increment("rate_limit:com.example.auth_service.controller.AuthController:login:127.0.0.1");
    }

    @Test
    @DisplayName("Должен сбрасывать счетчик после истечения временного окна")
    void testRateLimitResetAfterTimeWindow() throws Throwable {
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(1);
        when(rateLimit.key()).thenReturn("");
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(joinPoint.proceed()).thenReturn("success");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);

        Thread.sleep(1100);

        when(valueOperations.increment(anyString())).thenReturn(1L);
        Object result2 = rateLimitAspect.rateLimit(joinPoint, rateLimit);

        assertEquals("success", result);
        assertEquals("success", result2);
    }

    @Test
    @DisplayName("Должен обрабатывать разные IP-адреса независимо")
    void teatDifferentIpAddresses() throws Throwable {
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(60);
        when(rateLimit.key()).thenReturn("");
        when(joinPoint.proceed()).thenReturn("success");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);

        // Первый IP
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        when(valueOperations.increment(anyString())).thenReturn(6L);

        assertThrows(RateLimitExceededException.class, () -> {
            rateLimitAspect.rateLimit(joinPoint, rateLimit);
        });

        // Второй IP
        when(request.getRemoteAddr()).thenReturn("127.0.0.2");
        when(valueOperations.increment(anyString())).thenReturn(1L);

        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);
        assertEquals("success", result);
    }

    @Test
    @DisplayName("Должен обрабатывать разные методы независимо")
    void tastDifferentMethods() throws Throwable {
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(60);
        when(rateLimit.key()).thenReturn("");
        when(joinPoint.proceed()).thenReturn("success");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // Первый метод
        when(signature.getName()).thenReturn("login");
        when(valueOperations.increment(anyString())).thenReturn(6L);

        assertThrows(RateLimitExceededException.class, () -> {
            rateLimitAspect.rateLimit(joinPoint, rateLimit);
        });

        when(signature.getName()).thenReturn("register");
        when(valueOperations.increment(anyString())).thenReturn(1L);

        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);
        assertEquals("success", result);
    }

    @Test
    @DisplayName("Должен корректно работать с разными временными окнами")
    void testDifferentTimeWindows() throws Throwable {
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(30);
        when(rateLimit.key()).thenReturn("");
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(joinPoint.proceed()).thenReturn("success");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);

        assertEquals("success", result);
        verify(rateLimitRedisTemplate).expire(anyString(), eq(30L), any());

    }

    @Test
    @DisplayName("Должен разрешить запрос на границе лимита")
    void testAtLimitBoundary() throws Throwable {
        // Arrange
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.key()).thenReturn("");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        when(valueOperations.increment(anyString())).thenReturn(5L);
        when(joinPoint.proceed()).thenReturn("success");

        // Act
        Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);

        // Assert
        assertEquals("success", result);
    }

    @Test
    @DisplayName("Должен выбросить исключение при превышении лимита на 1")
    void testExceedingLimitByOne() {
        // Arrange
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.key()).thenReturn("");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        when(valueOperations.increment(anyString())).thenReturn(6L);

        // Act & Assert
        assertThrows(RateLimitExceededException.class, () -> {
            rateLimitAspect.rateLimit(joinPoint, rateLimit);
        });
    }

    @Test
    @DisplayName("Должен корректно обрабатывать параллельные запросы")
    void testConcurrentRequests() throws Throwable {
        // Arrange
        RateLimit rateLimit = mock(RateLimit.class);
        when(rateLimit.value()).thenReturn(5);
        when(rateLimit.timeWindow()).thenReturn(60);
        when(rateLimit.key()).thenReturn("");
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getDeclaringTypeName()).thenReturn("com.example.auth_service.controller.AuthController");
        when(signature.getName()).thenReturn("login");
        RequestContextHolder.setRequestAttributes(requestAttributes);
        when(requestAttributes.getRequest()).thenReturn(request);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // Имитация параллельных запросов
        when(valueOperations.increment(anyString())).thenReturn(1L, 2L, 3L, 4L, 5L, 6L);
        when(joinPoint.proceed()).thenReturn("success");

        // Выполняем 5 запросов (в пределах лимита)
        for (int i = 0; i < 5; i++) {
            Object result = rateLimitAspect.rateLimit(joinPoint, rateLimit);
            assertEquals("success", result);
        }

        // Шестой запрос должен превысить лимит
        assertThrows(RateLimitExceededException.class, () -> {
            rateLimitAspect.rateLimit(joinPoint, rateLimit);
        });
    }


}