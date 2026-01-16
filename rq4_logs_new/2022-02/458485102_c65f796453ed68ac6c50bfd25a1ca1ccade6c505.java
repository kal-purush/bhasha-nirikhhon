package org.example.projectapp.config;

import org.example.projectapp.auth.AuthService;
import org.example.projectapp.permission.ProjectPermissionEvaluatorBean;
import org.example.projectapp.service.ProjectMemberService;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.access.expression.method.MethodSecurityExpressionHandler;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.method.configuration.GlobalMethodSecurityConfiguration;

@Configuration
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class MethodSecurityConfig extends GlobalMethodSecurityConfiguration {
    private final ProjectMemberService projectMemberService;
    private final AuthService authService;

    public MethodSecurityConfig(ProjectMemberService projectMemberService, AuthService authService) {
        this.projectMemberService = projectMemberService;
        this.authService = authService;
    }

    @Override
    protected MethodSecurityExpressionHandler createExpressionHandler() {
        DefaultMethodSecurityExpressionHandler expressionHandler =
                new DefaultMethodSecurityExpressionHandler();
        expressionHandler.setPermissionEvaluator(new ProjectPermissionEvaluatorBean(authService, projectMemberService));
        return expressionHandler;
    }
}