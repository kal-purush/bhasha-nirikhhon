package com.example.hello_spring.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component // 이렇게 해도된고 스프링빈에 등록해서 사용해도된다.
public class TimeTraceAop {

  @Around("execution(* com.example.hello_spring..*(..))")//타겟팅해주는거나
  public Object execute(ProceedingJoinPoint joinPoint) throws Throwable{
    // 시간로직
    long start = System.currentTimeMillis();
    System.out.println("start :::::::::::::::::::: " + joinPoint.toString());
    try {
      return joinPoint.proceed();// 다음메서드로 진행
    } finally {
      long finish = System.currentTimeMillis();
      long timeMs = finish - start;
      System.out.println("end ::::::::::::: " + joinPoint.toString() + "  " + timeMs + "ms");
    }

  }

}