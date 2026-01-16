package org.example.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.example.service.AuditService;
import org.example.model.User;
import org.example.util.AuditLog;

import java.time.LocalDateTime;

@Aspect
public class AuditAspect {

    private final AuditService auditService;
    private final User currentUser; // You should implement a way to get the current user

    public AuditAspect(AuditService auditService, User currentUser) {
        this.auditService = auditService;
        this.currentUser = currentUser;
    }

    @Pointcut("execution(* org.example.service.*.*(..))")
    public void serviceLayer() {}

    @AfterReturning(pointcut = "serviceLayer()")
    public void logAfterMethodExecution(JoinPoint joinPoint) {
        String action = joinPoint.getSignature().getName();
        String methodName = joinPoint.getSignature().toShortString();

        // Create audit log
        AuditLog logEntry = new AuditLog();
        logEntry.setUser(currentUser);
        logEntry.setAction("Executed method: " + methodName);
        logEntry.setTimestamp(LocalDateTime.now());

        // Save the log
        auditService.logAction(currentUser, action);
    }
}