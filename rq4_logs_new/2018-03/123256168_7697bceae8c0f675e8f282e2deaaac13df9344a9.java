package com.kgisl.Eventhandler.Eventhandler.aspect;

import java.util.Arrays;

import org.aopalliance.aop.Advice;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.ThrowsAdvice;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class AspectLog implements ThrowsAdvice {
    //private static final Logger LOGGER = LoggerFactory.getLogger(TodoAspect.class);
    Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Pointcut("execution(* com.kgisl.Eventhandler.Eventhandler.service.*.*(..))")
    public void controller() {
    }

    @Pointcut("execution(* com.kgisl.Eventhandler.Eventhandler.service.*.*(..))")
    protected void allMethod() {
    }

    @Before("execution(* com.kgisl.Eventhandler.Eventhandler.service.*.*(..))")
    public void beforelog(JoinPoint point) {
        LOGGER.info("-----------------------@Before advice called--------------------------------------");
        LOGGER.debug("Class Name :  " + point.getSignature().getDeclaringTypeName());
        LOGGER.info("Entering in Method :  " + point.getSignature().getName());
        LOGGER.warn("Argumentsttt :  " + Arrays.toString(point.getArgs()));
    }

    @AfterReturning(pointcut = "within(@org.springframework.stereotype.Service *)", returning = "result")
    public void logAfterReturning(JoinPoint joinPoint, Object result) {
        LOGGER.info("------------------- @AfterReturning advice called ---------------------------------");
        LOGGER.info(" >Returning for class : {} ; Method : {} ", joinPoint.getTarget().getClass().getName(),
                joinPoint.getSignature().getName());
        if (result != null) {
            LOGGER.info("> with value : {}", result.toString());
        } else {
            LOGGER.info(">with null as return value.");
        }
    }
    
    @AfterThrowing(pointcut = "within(@org.springframework.stereotype.Service *||@org.springframework.stereotype.Controller.divideByZero * )", throwing = "exception")
    public void logAfterThrowing(JoinPoint joinPoint, Throwable exception)throws Throwable {
        System.out.println("++++++++++++please enter into throwing+++++++++++++");
        LOGGER.info("-------------------@AfterThrowing advice called -----------------------------------");
        LOGGER.error(">>Exception Thrown: " + exception);
        LOGGER.error(">>Inside CatchThrowException.afterThrowing() method...");
        LOGGER.error(">>Running after throwing exception...");
      
    }
    @AfterThrowing(pointcut="execution(* com.kgisl.Eventhandler.Eventhandler.Controller.*.*(..))", 
        throwing="excep")
        public void afterThrowing(JoinPoint joinPoint, Throwable excep) throws Throwable {
            LOGGER.info("------------------ @AFter Throwing advice called ----------------------------------------");
            LOGGER.error(">>Exception Illegal Arithmetic Exception argument:: " + excep);
            System.out.println("Inside CatchThrowException.afterThrowing() method...");
            System.out.println("Running after throwing exception...");
            System.out.println("Exception : " + excep);
           ]
        }
        

    // @Around("execution(* com.kgisl.Eventhandler.Eventhandler.service.*.*(..))")
    // public Object logAround(ProceedingJoinPoint joinPoint) throws Throwable{
    //     LOGGER.info("------------------ @Around advice called ----------------------------------------");
    //     LOGGER.info("The method "+joinPoint.getSignature().getName()+"() begins with "
    //             +Arrays.toString(joinPoint.getArgs()));
    //     try{
    //         Object result = joinPoint.proceed();
    //         LOGGER.info("The method "+joinPoint.getSignature().getName()
    //                 +"() ends with "+result);
                  
    //         return result;
    //     }catch(IllegalArgumentException e){
    //         // LOGGER.error("Illegal argument "+Arrays.toString(joinPoint.getArgs())
    //         //         +" in "+joinPoint.getSignature().getName()+"()");
    //         // throw e;
    //         LOGGER.error(">>Illegal argument:");
    //         LOGGER.error(">>Exception Illegal argument:: " + e);

    // throw e;
    //     }
    // }

  

}