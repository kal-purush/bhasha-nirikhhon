package com.shufan.usercenter.common;

import haiyan.cache.CacheUtil;
import haiyan.cache.RedisStringDataCache;
import haiyan.common.DebugUtil;
import haiyan.common.LogUtil;
import haiyan.common.PropUtil;
import haiyan.common.VarUtil;
import haiyan.common.cache.AppDataCache;
import haiyan.common.intf.ILogger;
import haiyan.common.intf.cache.IDataCache;
import haiyan.config.util.ConfigUtil;
import haiyan.exp.ExpUtil;

import java.io.File;
import java.net.URL;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;


public class ContextListener implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent event) {
	}
	@Override
	public void contextInitialized(ServletContextEvent event) {
		String webInfPath = event.getServletContext().getRealPath("WEB-INF");
		init(webInfPath);
	}
	// 使用集群服务
	public static boolean USE_ES = VarUtil.toBool(PropUtil.getProperty("search.engine"));
	public static boolean USE_CACHE = VarUtil.toBool(PropUtil.getProperty("cache.engine"));
	public static boolean USE_DEVLOGGER = VarUtil.toBool(PropUtil.getProperty("logger.develop"));
	public static void init(String webInfPath) {
		initLogger();
		initCache();
		initTables(webInfPath);
		DebugUtil.debug("contextInitialized");
	}
	private static void initLogger() {
		if (USE_DEVLOGGER==false)
			DebugUtil.logger = new ILogger() { // 强制设置logger接口
	            @Override
	            public void debug(Object info) {
	                LogUtil.info(info);
	            }
	            @Override
	            public void error(Object info, Throwable ex) {
	                LogUtil.error(info, ex);
	            }
	            @Override
	            public void error(Object info) {
	                if (info instanceof Throwable) {
	                    Throwable ex = (Throwable)info;
	                    LogUtil.error(ex.getMessage(), ex);
	                } else
	                    LogUtil.error(info);
	            }
	            @Override
	            public void warn(Object info) {
	                LogUtil.warn(info);
	            }
	        };
	}
	private static void initCache() {
		IDataCache cache = null;
		if (USE_CACHE) {
//			cache = new RedisBinaryDataCacheRemote();
			cache = new RedisStringDataCache();//此缓存存取都是string
			String servers = PropUtil.getProperty("REDISCACHE.SERVERS");
			cache.setServers(servers.split(";"));
	    	try {
	    		cache.initialize();
	    	}catch(Throwable e){
	    		DebugUtil.error(e);
	    		System.exit(0);
	    	}
		} else {
//			cache = new EHDataCache();
			cache = new AppDataCache();
		}
		CacheUtil.setDataCache(cache); // 全局用缓存框架
		ConfigUtil.setDataCache(new AppDataCache()); // 配置用缓存框架
		ConfigUtil.setExpUtil(new ExpUtil()); // 全局用公式引擎
		ConfigUtil.setORMUseCache(true); // 开启ORM自动多级缓存(根据每个缓存级别来使用DataCache实现)
	}
	private static void initTables(String webInfPath) {
		String hyHome = new File(webInfPath).getParent();
		System.setProperty("HAIYAN_HOME", hyHome);
		String rootName = webInfPath + File.separator + "haiyan-config.xml";
		File file = new File(rootName);
		try {
			ConfigUtil.loadRootConfig(file);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		String[] configNames = new String[]{
				"SYS.xml","SYSCACHE.xml","SYSORGA.xml","SYSROLE.xml","SYSOPERATOR.xml","SYSUSERROLE.xml"
				,"T_UC_ADDRESS.xml","T_POINTS_TOTALPOINTS.xml"};
		for (String configName:configNames) {
			URL url = ContextListener.class.getClassLoader().getResource(configName);
			if (url==null)
				throw new RuntimeException("config not found:"+configName);
			file = new File(url.getPath());
			if (!file.exists()) {
				throw new RuntimeException("file not found:"+file.getAbsolutePath());
			}
			ConfigUtil.loadTableConfig(file, true);
		}
		{//初始化Bill
			URL url = ContextListener.class.getClassLoader().getResource("B_UC_USER.xml");
			if (url==null)
				throw new RuntimeException("config not found:"+"B_UC_USER.xml");
			file = new File(url.getPath());
			if (!file.exists()) {
				throw new RuntimeException("file not found:"+file.getAbsolutePath());
			}
			ConfigUtil.loadBillConfig(file, true);
		}
	}

}