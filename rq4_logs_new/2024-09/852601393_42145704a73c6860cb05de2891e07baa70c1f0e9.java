package com.boot.Service;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.boot.DAO.PricetbDAO_2;
import com.boot.DTO.PricetbDTO;

import lombok.extern.slf4j.Slf4j;

@Service("PricetbService_2")
@Slf4j
public class PricetbServiceIpml_2 implements PricetbService_2 {
	
	@Autowired
	private SqlSession sqlSession;
	
	@Override
	public ArrayList<Integer> selectprice(HashMap<String, String> param) {
		PricetbDAO_2 dao = sqlSession.getMapper(PricetbDAO_2.class);
		
		return dao.selectprice(param);
	}
}