package com.password.action;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.password.service.UserInfoService;
import com.password.vo.UserInfo;

@Controller
@RequestMapping(value="/user")
public class UserInfoAction {
	
	@Autowired
	private UserInfoService userInfoService;
	
	/**
	 * 得到所有用户信息
	 * @param map
	 * @return
	 */
	@RequestMapping(value="/getAllUser")
	public String getAllUser(Map<String, Object> map){
		List<UserInfo> userList = userInfoService.findAll();
		map.put("ALLUSER", userList);
		return "allUser";
	}

	/**
	 * 添加用户操作
	 * @param userinfo
	 * @return
	 */
	@RequestMapping(value="/addUser",method=RequestMethod.POST)
	public String save(UserInfo userinfo){
		int result = userInfoService.insert(userinfo);
		System.out.println("添加用户的操作结果为："+result);
		return "redirect:/user/getAllUser";
	}
}