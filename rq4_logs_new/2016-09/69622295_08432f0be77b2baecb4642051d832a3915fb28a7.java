package com.stone.base.controller;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.annotation.Resource;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.web.servlet.SimpleCookie;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;

import com.stone.base.service.IMybatisBaseService;
import com.stone.base.system.SystemConstants;
import com.stone.base.system.SystemConstants.CacheKey;
import com.stone.base.system.SystemConstants.CookieSet;
import com.stone.cart.model.Cart;
import com.stone.user.model.TAdmin;
import com.stone.user.model.TUser;
import com.stone.util.MD5Util;
import com.stone.util.PathUtil;

/**
 * 
 * @author stone
 *
 */
@Controller
public class BaseController {

	protected static final Logger log = Logger.getLogger(BaseController.class);

	/**
	 * 后台登陆权限认证
	 */
	protected static final String admin = "adm";
	/**
	 * 前台登陆权限认证
	 */
	protected static final String auth = "auth";

	@Resource(name = "redisTemplate")
	private RedisTemplate<Object,Object> redisTemplate;

	@Resource(name = "mybatisBaseService")
	private IMybatisBaseService mybatisBaseService;

	public IMybatisBaseService getMybatisBaseService() {
		return mybatisBaseService;
	}

	public void setMybatisBaseService(IMybatisBaseService mybatisBaseService) {
		this.mybatisBaseService = mybatisBaseService;
	}

	/**
	 * 获取日志对象
	 * 
	 * @return
	 */
	protected Logger getLogger() {
		return Logger.getLogger(this.getClass());
	}

	
	protected TUser getCurrentUser(HttpServletRequest request) {
		String token = CacheKey.user + "" + getCurrentUserId(request);
		TUser user = (TUser) redisTemplate.getUserRedisTemplate().getObjectFromRedis(token);
		return user;
	}
	
	protected void removeCurrentUser(HttpServletRequest request) {
		String token = CacheKey.user + "" + getCurrentUserId(request);
		redisTemplate.getUserRedisTemplate().delete(token);
	}
	
	
	protected void setCurrentUser(TUser user,HttpServletRequest request, HttpServletResponse response) {
		String token = user.getUserId().toString();
		setCookie(response, CookieSet.tokenNname.toString(),token);
		token = CacheKey.user+token;
		redisTemplate.getUserRedisTemplate().setObjectIntoRedis(token, user);
	}
	
	protected int getCurrentUserId(HttpServletRequest request) {
		return Integer.parseInt(getCookie(request,CookieSet.tokenNname.toString()));
	}
	
	protected int getShopId(HttpServletRequest request){
		return getCurrentUser(request).getShopId();
	}

	
	protected Cart getCurrentCart(HttpServletRequest request) {
		String token = CacheKey.cart + "" + getCurrentUserId(request);
		Cart cart = (Cart) redisTemplate.getCartRedisTemplate().getObjectFromRedis(token);
		return cart;
	}
	
	protected void setCurrentCart(Cart cart,HttpServletRequest request, HttpServletResponse response) {
		String token = CacheKey.cart + "" + getCurrentUserId(request);
		
		redisTemplate.getCartRedisTemplate().setObjectIntoRedis(token, cart);
	}
	
	
	protected TAdmin getCurrentAdmin(HttpServletRequest request) {
		TAdmin adm = null;
		Object o = request.getSession().getAttribute(SystemConstants.SESSION_ADMIN);
		if (o != null) {
			adm = (TAdmin) o;
		}
		return adm;
	}
	
	protected int getCurrentAdminId(HttpServletRequest request) {
		int id = 0;
		Object o = request.getSession().getAttribute(SystemConstants.SESSION_ADMIN);
		if (o != null) {
			TAdmin a = (TAdmin) o;
			id = a.getId();
		}
		return id;
	}


	protected void setCurrentAdmin(HttpServletRequest request, TAdmin adm) {
		request.getSession().setAttribute(SystemConstants.SESSION_ADMIN, adm);
	}

	protected void userLogout4redis(HttpServletRequest request) {
		String token = this.getCookie(request, "token");
		redisTemplate.delete(token);
	}

	protected void adminLogout(HttpServletRequest request) {
		request.getSession().removeAttribute(SystemConstants.SESSION_ADMIN);
	}

	private final static PathUtil pathUtil = new PathUtil();

	private final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	protected String getCurrentTime() {
		return timeFormat.format(new Date());
	}

	protected String getCurrentDate() {
		return dateFormat.format(new Date());
	}


	public String getIpAddr(HttpServletRequest request) {
		String ip = request.getHeader("x-forwarded-for");
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}
		return ip;
	}

	protected String getIpAddr2(HttpServletRequest request) {
		String ipAddress = null;
		// ipAddress = this.getRequest().getRemoteAddr();
		ipAddress = request.getHeader("x-forwarded-for");
		if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
			ipAddress = request.getHeader("Proxy-Client-IP");
		}
		if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
			ipAddress = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
			ipAddress = request.getRemoteAddr();
			if (ipAddress.equals("127.0.0.1")) {
				// 根据网卡取本机配置的IP
				InetAddress inet = null;
				try {
					inet = InetAddress.getLocalHost();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
				ipAddress = inet.getHostAddress();
			}

		}
		// 对于通过多个代理的情况，第一个IP为客户端真实IP,多个IP按照','分割
		if (ipAddress != null && ipAddress.length() > 15) { // "***.***.***.***".length()
															// = 15
			if (ipAddress.indexOf(",") > 0) {
				ipAddress = ipAddress.substring(0, ipAddress.indexOf(","));
			}
		}
		return ipAddress;
	}

	protected void setCookie(HttpServletResponse response, String name, String value) {

		Cookie cookie = new Cookie(name, value);
		cookie.setMaxAge(60 * 60 * 24 * 365);
//		cookie.setDomain(CookieSet.domain.toString());
		cookie.setPath(CookieSet.path.toString());
		response.addCookie(cookie);

	}
	protected String getCookie(HttpServletRequest request, String name) {
		String value = "0";
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (int i = 0; i < cookies.length; i++) {
				Cookie c = cookies[i];
				if (c.getName().equalsIgnoreCase(name)) {
					value = c.getValue();
					break;
				}
			}
		}
		return value;
	}


	/**
	 * 域名
	 */
	public static final String domain = "/veryfine";

	public RedisTemplate<Object, Object> getRedisTemplate() {
		return redisTemplate;
	}

	public void setRedisTemplate(RedisTemplate<Object, Object> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}


	
	

}