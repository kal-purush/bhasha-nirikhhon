package com.island.app.admin.service;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.island.app.admin.dao.AdminDao;
import com.island.app.admin.vo.AdminVo;

@Service
@Transactional
public class AdminService {
	private final SqlSessionTemplate sst;
	private final AdminDao dao;

	@Autowired
	public AdminService(SqlSessionTemplate sst, AdminDao dao) {
		this.sst = sst;
		this.dao = dao;
	}

	// 로그인
	public AdminVo login(AdminVo vo) {
		return dao.login(sst, vo);
	}
}