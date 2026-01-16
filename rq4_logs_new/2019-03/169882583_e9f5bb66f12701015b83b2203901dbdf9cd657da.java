package ru.aaxee.homework2.aspect;

import lombok.extern.java.Log;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import ru.aaxee.homework2.dto.Response;
import ru.aaxee.homework2.exception.LibraryException;

import java.util.logging.Level;

@Log
@Aspect
@Component
public class ShellMethodExceptionHandlerAspect {

    @Pointcut("@annotation(org.springframework.shell.standard.ShellMethod)")
    public void isShellMethodAnnotation() {
    }

    @Pointcut("execution(ru.aaxee.homework2.dto.Response *..*ShellController.*(..))")
    public void isReturnResponseInShellController() {
    }

    @Around("isShellMethodAnnotation() && isReturnResponseInShellController()")
    public Response exceptionToResponseAdvice(ProceedingJoinPoint joinPoint) {
        Response response;
        try {
            response = (Response) joinPoint.proceed();
        } catch (LibraryException ex) {
            response = Response.error(ex.getLocalizedMessage());
        } catch (Throwable ex) {
            String message = "Unknown internal error";
            log.log(Level.SEVERE, message, ex);
            response = Response.error(message + ". " + ex.getLocalizedMessage());
        }
        return response;
    }
}