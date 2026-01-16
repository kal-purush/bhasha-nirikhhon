package com.common.log;

import org.apache.log4j.Logger;

/**
 * 提供统一写异常日志的功能
 * @author yuqing
 * </br>Date 2014-05-10
 */
public class SystemLogger{	
	/**
	 * 致命错误
	 */
	public final static  int FATAL=5;
	/**
	 * 系统错误
	 */
	public final static int ERROR=4;
		/**
		 * 功能警告
		 */
	public final static int WARN=3;
		/**
		 * 运行信息
		 */
	public final static int INFO=2;
		/**
		 *  调试信息
		 */
	public final static int DEBUG=1;
	
	/**
	 * 根据指定日志等级，写日志
	 * @param grad 日志等级，包括ExceptionLogger.DEBUG、INFO、WARN、ERROR、FATAL
	 * @param logContent
	 * @param exception 导常对象，如没有，可设置为null
	 * @param source 调用日志的类型
	 */
	public static void writeLog(int grad,String logContent,Exception exception,Class clazz){
		//LOG4J实现
		switch(grad){
			case 1:Logger.getLogger(clazz).debug(logContent,exception);break;
			case 2:Logger.getLogger(clazz).info(logContent,exception);break;
			case 3:Logger.getLogger(clazz).warn(logContent,exception);break;
			case 4:Logger.getLogger(clazz).error(logContent,exception);break;
			case 5:Logger.getLogger(clazz).fatal(logContent,exception);break;
		}
	}
	
	/**
	 * 写错误日志,隔离具体体的log实现组件:
	 * @param e 异常实例
	 * @param source 发生异常时对象实例
	 */
	public static void writeLog(Exception e,Object source){
		//LOG4J实现
		Logger.getLogger(source.getClass()).error("异常：",e);
	}
	
	/**
	 * 写错误日志,隔离具体体的log实现组件:
	 * @param e 异常实例
	 * @param clazz 发生异常时类型实例
	 */
	public static void writeLog(Exception e,Class clazz){
		//LOG4J实现
		Logger.getLogger(clazz).error("异常：",e);
	}
}