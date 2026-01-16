package com.zcc.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.zcc.login.mapper.UserInfoMapper;
import com.zcc.login.model.User;
import com.zcc.login.vo.SelectUserRequest;

/**
 * Created by ZhangChicheng on 2017/11/8.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class UserInfoMapperTest {
	@Autowired
	UserInfoMapper userInfoMapper;

	@Test
	public void getUserTest(){
		SelectUserRequest request = new SelectUserRequest();
		request.setUser_name("lihao");
		User user = userInfoMapper.getUser(request);
		String id = user.getUser_id();
	}
}