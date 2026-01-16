package com.prime.aspects;

import com.prime.annotations.Audited;
import com.prime.entities.Project;
import com.prime.entities.Task;
import com.prime.models.response.ProjectResponse;
import com.prime.models.response.TaskResponse;
import com.prime.repositories.ProjectRepository;
import com.prime.repositories.TaskRepository;
import com.prime.service.ActivityLogService;
import com.prime.utils.SecurityUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hibernate.Hibernate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Method;
import java.util.UUID;

@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
public class AuditAspect {

    private final ActivityLogService activityLogService;
    private final ProjectRepository projectRepository;
    private final TaskRepository taskRepository;

    @Around("@annotation(com.prime.annotations.Audited)")
    @Transactional
    public Object auditMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        // Get the method and annotation
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Audited auditAnnotation = method.getAnnotation(Audited.class);

        // Get method arguments
        Object[] args = joinPoint.getArgs();
        Object oldEntity = null;
        UUID projectId = null;
        UUID taskId = null;

        // Extract entity information before the method execution
        if (auditAnnotation.entityType().equals("PROJECT")) {
            if (method.getName().contains("update") || method.getName().contains("delete")) {
                UUID id = (UUID) args[method.getName().contains("update") ? 1 : 0];
                oldEntity = projectRepository.findById(id).orElse(null);
                if (oldEntity != null) {
                    projectId = id;
                }
            }
        } else if (auditAnnotation.entityType().equals("TASK")) {
            if (method.getName().contains("update") || method.getName().contains("delete")) {
                UUID id = (UUID) args[method.getName().contains("update") ? 1 : 0];
                Task task = taskRepository.findById(id).orElse(null);
                if (task != null) {
                    Hibernate.initialize(task.getProject());
                    oldEntity = task;
                    projectId = task.getProject().getId();
                    taskId = task.getId();
                }
            }
        }

        // Execute the method
        Object result = joinPoint.proceed();

        try {
            // Build description
            String description = auditAnnotation.description();
            if (description.isEmpty()) {
                description = buildDefaultDescription(method.getName(), result, oldEntity);
            }

            // If it's a create operation, extract IDs from the result
            if (method.getName().contains("create")) {
                if (result instanceof ProjectResponse) {
                    projectId = ((ProjectResponse) result).getId();
                } else if (result instanceof TaskResponse) {
                    TaskResponse task = (TaskResponse) result;
                    projectId = task.getProjectId();
                    taskId = task.getId();
                }
            }

            // Log the activity
            activityLogService.logActivity(
                SecurityUtil.getIDUser().toString(),
                projectId != null ? projectId.toString() : null,
                auditAnnotation.activityType()
            );
        } catch (Exception e) {
            log.error("Failed to log activity", e);
        }

        return result;
    }

    private String buildDefaultDescription(String methodName, Object result, Object oldEntity) {
        if (methodName.contains("create")) {
            if (result instanceof ProjectResponse) {
                return "Project created: " + ((ProjectResponse) result).getName();
            } else if (result instanceof TaskResponse) {
                return "Task created: " + ((TaskResponse) result).getTitle();
            }
        } else if (methodName.contains("update")) {
            if (result instanceof ProjectResponse) {
                return "Project updated: " + ((ProjectResponse) result).getName();
            } else if (result instanceof TaskResponse) {
                return "Task updated: " + ((TaskResponse) result).getTitle();
            }
        } else if (methodName.contains("delete")) {
            if (oldEntity instanceof Project) {
                return "Project deleted: " + ((Project) oldEntity).getName();
            } else if (oldEntity instanceof Task) {
                return "Task deleted: " + ((Task) oldEntity).getTitle();
            }
        }
        return methodName;
    }
} 