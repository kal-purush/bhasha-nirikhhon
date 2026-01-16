package org.charan.logging;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class LoggingAspect {
	@Before("duallogs()")
	public void LoggingAdvice(){
		System.out.println("Advice Run.Get method is called");
	}
	
	@Before("duallogs()")
	public void secondAdvice(){
		System.out.println("Second Advice.");
	}
	
	@Pointcut("execution(* get*())")
	public void duallogs(){}
}