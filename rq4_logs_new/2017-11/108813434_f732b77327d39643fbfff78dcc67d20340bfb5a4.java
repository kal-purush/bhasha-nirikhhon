package com.zcc.login.service;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.zcc.login.constant.ErrorCodeEnum;
import com.zcc.login.exception.ServiceException;
import com.zcc.login.model.User;
import com.zcc.login.vo.CreateUserRequest;
import com.zcc.login.vo.SelectUserRequest;

/**
 * Created by ZhangChicheng on 2017/11/8.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserInfoMapperTest {
	@Autowired
	UserService userService;

	@Test
	public void getUserTest() {
		String userName = "zhangchicheng";
		SelectUserRequest request = new SelectUserRequest();
		request.setUserName(userName);
		User user = userService.findUser(request);
		assertNotNull(user);
		assertEquals(user.getUserName(),userName);
	}

	@Test
	public void createUserTest(){
		CreateUserRequest request = new CreateUserRequest();
		request.setUserName("test");
		request.setPassword("123");
		request.setEmail("abc@123.com");
		request.setPhoneNum("111");
		User user;
		try{
			user = userService.createUser(request);
			assertNotNull(user);
			assertNotNull(user.getAuthorities());
			assertEquals(request.getUserName(),user.getUserName());
			userService.createUser(request);
		}catch (ServiceException e){
			assertEquals(e.getErrorCode(), ErrorCodeEnum.USER_NAME_INVALID.getErrorCode());
		}

	}
}