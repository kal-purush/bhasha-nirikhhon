package com.xuzj.listen;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * 
web监听器是servlet规范中定义的一种特殊类，用于监听ServletContext、HttpSession、ServletRequest
等域对象的创建于销毁事件。用于监听域对象的属性发生修改的事件，可以在事件发生前、发生后做一些必要的处理。

用途：统计在线人数和在线用户、系统启动时加载初始化信息、统计网站访问量、跟Spring结合。

步骤：一 创建一个实现监听器接口的类 2配置web.xml进行注册

 * 一个web.xml下多个监听器，按照注册顺序加载监听器。
 * 监听器》过滤器》servlet
 * 
 * 监听器分类：
 * 按监听的对象划分： 
 * 	1、用于监听应用程序环境变量(ServletContext)的事件监听器
 *  2、用于监听会话对象（HttpSession）的事件监听器
 *  3、用于监听请求消息对象（ServletRequest）的事件监听器
 *  
 * 按监听的事件划分：
 *  1、监听域对象自身的创建和销毁的事件监听器（ServletContext HttpSession ServletRequest）
 *  2、监听域对象中的属性的增加和删除的事件监听器
 *  3、监听绑定到HttpSession域中的某个对象的状态的事件监听器
 *  
 *  ServletContextListener用途：定时器，全局属性对象（创建全局数据库连接）
 *  HttpSession 用途：统计在线人数，记录访问日志
 *  ServletRequestListen用途：读取参数，记录访问历史
 * @author xuzj
 *
 */
public class FirstListen implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent) {
		// TODO Auto-generated method stub
		System.out.println("contextDestroyed...");
	}

	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent) {
		// TODO Auto-generated method stub
		//这个值在web.xml中的context-param
		String param = servletContextEvent.getServletContext().getInitParameter("servletContextEventInitParameter");
		//servletContextEvent.getServletContext().setAttribute(arg0, arg1) 放置全局的属性对象
		System.out.println("contextInitialized..."+param);
	}

}