package com.island.app.seminar.Service;

import java.util.List;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.island.app.seminar.bank.vo.BankVo;
import com.island.app.seminar.dao.SeminarDao;

@Service
@Transactional
public class SeminarService {
	private final SqlSessionTemplate sst;
	private final SeminarDao dao;

	@Autowired
	public SeminarService(SqlSessionTemplate sst, SeminarDao dao) {
		this.sst = sst;
		this.dao = dao;
	}
	
	//은행명 카테고리 조회
	public List<BankVo> bankCategorySelect(){
		return dao.bankCategorySelect(sst);
	}
	
}