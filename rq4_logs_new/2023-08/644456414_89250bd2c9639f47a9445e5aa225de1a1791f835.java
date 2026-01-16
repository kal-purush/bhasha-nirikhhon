package com.project.ipyang.aspect;

import com.project.ipyang.common.response.ResponseDto;
import com.project.ipyang.config.SessionUser;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpSession;

@Aspect
@Slf4j
@Component
public class LoginCheckAspect {
    private final HttpSession session;

    @Autowired
    public LoginCheckAspect(HttpSession session) {
        this.session = session;
    }


    @Around("execution(* com.project.ipyang.domain.board.controller.BoardController.writeBoard(..))|| execution(* com.project.ipyang.domain.board.controller.BoardController.updateBoard(..))")
    public Object aroundWriteBoardMethod(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // Perform login check only for writeBoard method
        SessionUser loggedInUser = (SessionUser) session.getAttribute("loggedInUser");
        if (loggedInUser == null) {
            return new ResponseDto("로그인이 필요합니다", HttpStatus.UNAUTHORIZED.value());
        }
        Long memberId = loggedInUser.getId();
        if (memberId == null) {
            return new ResponseDto("존재하지 않는 회원입니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }

        return proceedingJoinPoint.proceed();
    }
}