package com.example.team258.common.aop;

import com.example.team258.common.entity.User;
import com.example.team258.common.jwt.SecurityUtil;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.ui.Model;

import java.util.Optional;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

@Aspect
@Component
public class UserPermissionAspect {
//    @AfterReturning(pointcut = "within(com.example.team258..*) && @within(org.springframework.stereotype.Controller)", returning = "model")
//    public void addUserPermission(Model model) {
//        Optional<User> loginUser = Optional.of(SecurityUtil.getPrincipal().get());
//        System.out.println("AOP");
//        if (model != null)
//            model.addAttribute("user", loginUser.isPresent() ? loginUser.get() : null);
//    }

//    @After("execution(* com.example.team258..*(.., org.springframework.ui.Model)) && within(com.example.team258..*)")
//    public void addUserPermission(JoinPoint joinPoint) {
//        System.out.println("AOP After");
//        Object[] args = joinPoint.getArgs();
//        for (Object arg : args) {
//            if (arg instanceof Model) {
//                Model model = (Model) arg;
//                // 사용자 정보 추가
//                Optional<User> loginUser = Optional.of(SecurityUtil.getPrincipal().get());
//                model.addAttribute("user", loginUser.isPresent() ? loginUser.get() : null);
//                break;
//            }
//        }
//    }
}