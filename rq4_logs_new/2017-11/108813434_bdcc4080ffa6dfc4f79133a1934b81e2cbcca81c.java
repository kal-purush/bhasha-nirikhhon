package com.zcc.login.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.zcc.login.mapper.AuthorityMapper;
import com.zcc.login.model.Authority;
import com.zcc.login.service.AuthorityService;

/**
 * Created by ZhangChicheng on 2017/11/9.
 */
@Service
@SuppressWarnings("SpringJavaAutowiringInspection")
public class AuthorityServiceImpl implements AuthorityService{

	@Autowired
	AuthorityMapper authorityMapper;

	@Override
	public List<Authority> getUserAuthorities(String userName) {
		return authorityMapper.getUserAuthorities(userName);
	}

	@Override
	public int deleteAllAuthority(int userId) {
		return authorityMapper.deleteAllAuthority(userId);
	}

	@Override
	public int addAuthority(int userId, Authority authority) {
		return 0;
	}

	@Override
	public int deleteAuthority(int userId, Authority authority) {
		return 0;
	}
}