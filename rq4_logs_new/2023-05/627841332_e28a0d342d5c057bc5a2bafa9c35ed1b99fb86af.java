package com.island.app.member.service;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.island.app.member.dao.MemberDao;
import com.island.app.member.vo.MemberVo;
/**
 * 
 * @author 김수진
 *
 */
@Service
@Transactional
public class MemberService {
	
	private final SqlSessionTemplate sst;
	private final MemberDao dao;
	
	@Autowired
	public MemberService(SqlSessionTemplate sst , MemberDao dao) {
		this.sst = sst;
		this.dao = dao;

	}
	//회원가입
	public int join(MemberVo vo) throws Exception{
		//sql
		System.out.println(vo);
		return dao.join(sst, vo);
	}
	//아이디 중복확인
	public int checkId(String id) {
		return dao.checkId(sst , id);
	}
	
}