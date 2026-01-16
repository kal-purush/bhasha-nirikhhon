package com.boot.Service;

import java.util.HashMap;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.boot.DAO.PointDAO_2;
import com.boot.DTO.PointDTO;

import lombok.extern.slf4j.Slf4j;

@Service("PointService_2")
@Slf4j
public class PointServiceImpl_2 implements PointService_2 {
	@Autowired
	private SqlSession sqlSession;

	@Override
	public PointDTO getAllPoint(HashMap<String, String> param) {
		PointDAO_2 dao = sqlSession.getMapper(PointDAO_2.class);
		return dao.getAllPoint(param);
	}

	@Override
	public void Call_movie_payment_points(HashMap<String, String> param) {
		PointDAO_2 dao = sqlSession.getMapper(PointDAO_2.class);
		dao.Call_movie_payment_points(param);
	}
}