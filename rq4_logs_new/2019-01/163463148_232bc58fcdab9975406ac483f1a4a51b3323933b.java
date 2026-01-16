package cn.edu.tit.iservice;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import cn.edu.tit.bean.Admin;
import cn.edu.tit.bean.Category;


public interface IAdminService {
	
	/**
	 * 管理员初始化分类（系部）信息
	 */
	public void initializeCategory(List<Category> categories);
	
	/**
	 * 管理员修改分类（系部）信息
	 */
	public void modifyCategory();
	
	/**
	 * 超级管理员添加管理员
	 */
	public void addAdmin(List<Admin> admin);
	
	/**
	 * 超级管理员读取所有用户
	 */
	public List<Admin> readAdmin();
		
	/**
	 * 管理员导入用户（教师或学生）信息
	 * */
	public String addUserInfo(MultipartFile file);
}