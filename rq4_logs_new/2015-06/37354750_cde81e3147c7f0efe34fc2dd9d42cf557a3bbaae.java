package com.jjb.action;

import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.springframework.orm.hibernate4.support.HibernateDaoSupport;

import com.jjb.dao.ItemDao;
import com.jjb.dao.MoneyDao;
import com.jjb.dao.UserDao;
import com.jjb.util.JSONResponse;

/**
 * JJB Jersey Servlet类
 * @author Robert Peng
 */
@Path("/")
@Produces("application/json")
public class JJBAction extends HibernateDaoSupport {
	
	private ItemDao itemDao;
	private UserDao userDao;
	private MoneyDao moneyDao;
	
	/**
	 * 用户登录
	 * @param username 用户名
	 * @param password hash以后的32位密码
	 * @return
	 * 		登录成功则返回JSONResponse.signInSuccess，携带userId、accessKey与expiresTime
	 * 		登录失败则返回JSONResponse.fail
	 */
	@POST
	@Path("signIn")
	@Consumes("application/x-www-form-urlencoded")
	public String userSignIn(
			@FormParam("username") @DefaultValue("") String username,
			@FormParam("password") @DefaultValue("") String password) {
		if (username.isEmpty() || password.isEmpty() || !password.matches("[0-9a-f]{32}"))
			return JSONResponse.fail();
		// TODO to be implemented
		return JSONResponse.signInSuccess(0, "dummy", new Date());
	}
	
	/**
	 * 用户注册
	 * @param username 用户名
	 * @param password hash以后的32位密码
	 * @return
	 * 		注册成功则返回JSONResponse.signUpSuccess，携带userId、accessKey与expiresTime
	 * 		注册失败则返回JSONResponse.fail
	 */
	@POST
	@Path("signUp")
	@Consumes("application/x-www-form-urlencoded")
	public String userSignUp(
			@FormParam("username") @DefaultValue("") String username,
			@FormParam("password") @DefaultValue("") String password) {
		if (username.isEmpty() || password.isEmpty() || !password.matches("[0-9a-f]{32}"))
			return JSONResponse.fail();
		// TODO to be implemented
		return JSONResponse.signUpSuccess(0, "dummy", new Date());
	}
	
	/**
	 * 设定可用总额
	 * @param userId 用户id
	 * @param accessKey accessKey
	 * @param totalMoney 可用总额
	 * @return
	 * 		设定成功则返回JSONResponse.success
	 * 		设定失败则返回JSONResponse.fail
	 */
	@POST
	@Path("setting")
	@Consumes("application/x-www-form-urlencoded")
	public String setting(
			@FormParam("userId") int userId,
			@FormParam("accessKey") @DefaultValue("") String accessKey,
			@FormParam("totalMoney") double totalMoney) {
		if (userId < 1 || accessKey.isEmpty())
			return JSONResponse.fail();
		// TODO to be implemented
		return JSONResponse.success();
	}
	
	/**
	 * 从服务器下载指定日期后的消费记录，不指定日期则下载全部消费记录
	 * @param fromDateTime 指定日期，不指定（为空）则下载全部消费记录
	 * @return
	 * 		返回JSONResponse.syncData，携带items字段，值为Item组成的数组
	 */
	@GET
	@Path("sync")
	public String syncFromServer(
			@QueryParam("fromDateTime") Date fromDateTime) {
		// TODO to be implemented
		return null;
	}
	
	/**
	 * 向服务器上传消费记录
	 * @param items 由消费记录Item组成的JSON数组
	 * @return
	 * 		返回JSONResponse.syncOK，携带syncCount字段(成功上传的Item数)和lastModified字段(修改的服务器时间)
	 */
	@POST
	@Path("sync")
	@Consumes("application/x-www-form-urlencoded")
	public String syncToServer(
			@FormParam("items") @DefaultValue("") String items) {
		// TODO to be implemented
		return null;
	}
	
	public ItemDao getItemDao() {
		return itemDao;
	}
	
	public void setItemDao(ItemDao itemDao) {
		this.itemDao = itemDao;
	}
	
	public UserDao getUserDao() {
		return userDao;
	}
	
	public void setUserDao(UserDao userDao) {
		this.userDao = userDao;
	}
	
	public MoneyDao getMoneyDao() {
		return moneyDao;
	}
	
	public void setMoneyDao(MoneyDao moneyDao) {
		this.moneyDao = moneyDao;
	}

}