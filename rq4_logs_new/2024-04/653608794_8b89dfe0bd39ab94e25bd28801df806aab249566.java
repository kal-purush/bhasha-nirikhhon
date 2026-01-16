package com.xjdl.framework.aop;

public interface TargetSource {
	Class<?> getTargetClass();

	boolean isStatic();

	Object getTarget() throws Exception;

	void releaseTarget(Object target) throws Exception;
}