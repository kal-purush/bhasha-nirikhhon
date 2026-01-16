package com.boot.Service;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.boot.DAO.CouponDAO_2;
import com.boot.DTO.CouponDTO;

import lombok.extern.slf4j.Slf4j;

@Service("CouponService_2")
@Slf4j
public class CouponServiceImpl_2 implements CouponService_2 {

	@Autowired
	private SqlSession sqlSession;

	@Override
	public int checkCoupon(HashMap<String, String> param) {
		CouponDAO_2 dao = sqlSession.getMapper(CouponDAO_2.class);
		return dao.checkCoupon(param);
	}
	
	@Override
	public ArrayList<CouponDTO> getALLCoupon(HashMap<String, String> param) {
		CouponDAO_2 dao = sqlSession.getMapper(CouponDAO_2.class);
		return dao.getALLCoupon(param);
	}

	@Override
	public CouponDTO getCoupon(HashMap<String, String> param) {
		CouponDAO_2 dao = sqlSession.getMapper(CouponDAO_2.class);
		return dao.getCoupon(param);
	}

	@Override
	public void usingCoupon(HashMap<String, String> param) {
		CouponDAO_2 dao = sqlSession.getMapper(CouponDAO_2.class);
		dao.usingCoupon(param);
	}
}