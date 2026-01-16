package riovlev.aop.gradle.example.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;

import static riovlev.aop.gradle.example.aspect.Aspects.*;

/**
 * @author Roman Iovlev
 *         08/06/18
 */
@Aspect
public class ActionAspect {
    @Pointcut("execution(* *(..)) && @annotation(JLog)")
    protected void pointcut() { }

    @Before("pointcut()")
    public void before(JoinPoint joinPoint) {
        before.execute(joinPoint);
    }
    @AfterReturning(pointcut = "pointcut()", returning = "result")
    public void after(JoinPoint joinPoint, Object result) {
        after.execute(joinPoint, result);
    }
    /*@Around("pointcut()")
    public Object around(ProceedingJoinPoint pJoinPoint) {
        return around.execute(pJoinPoint);
    }*/
}