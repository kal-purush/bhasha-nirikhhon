package com.hafele.bos.dao;

import java.util.List;

import com.hafele.bos.dao.base.IBaseDao;
import com.hafele.bos.domain.Function;
/**
 * <p>Title: IFunctionDao</p>  
 * <p>Description: 权限管理Dao接口</p>  
 * @author Dragon.Wen
 * @date Aug 21, 2018
 */
public interface IFunctionDao extends IBaseDao<Function> {

	/**
	 * <p>Title: findByUserId</p>  
	 * <p>Description: 根据用户Id查询对应的权限</p>  
	 * @param id
	 * @return
	 */
	public List<Function> findFunctionListByUserId(String id);

	/**
	 * <p>Title: findAllMenu</p>  
	 * <p>Description: 查询所有菜单</p>  
	 * @return
	 */
	public List<Function> findAllMenu();

	/**
	 * <p>Title: findMenuByUserId</p>  
	 * <p>Description: 根据用户ID查询对应的基本功能菜单</p>  
	 * @param id
	 * @return
	 */
	public List<Function> findMenuByUserId(String id);

	/**
	 * <p>Title: findParentNodeAll</p>  
	 * <p>Description: 获取所有父节点菜单</p>  
	 * @return
	 */
	public List<Function> findParentNodeAll();

	/**
	 * <p>Title: findAllSystemMenu</p>  
	 * <p>Description: 查询所有管理员菜单</p>  
	 * @return
	 */
	public List<Function> findAllSystemMenu();

	/**
	 * <p>Title: findSystemMenuByUserId</p>  
	 * <p>Description: 根据管理员ID查询对应的系统管理员菜单</p>  
	 * @param id
	 * @return
	 */
	public List<Function> findSystemMenuByUserId(String id);

}