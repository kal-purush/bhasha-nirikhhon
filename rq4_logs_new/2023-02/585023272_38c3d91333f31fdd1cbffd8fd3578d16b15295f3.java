package com.spring_boot_momentor.dao;

import java.util.ArrayList;

import com.spring_boot_momentor.model.ServiceCenterVO;

public interface ISCDAO {
	public void insertServiceCenter(ServiceCenterVO vo); // 질문 등록
	public void updateServiceCenter(ServiceCenterVO vo); // 대답 등록
	
	public ArrayList<ServiceCenterVO> listAllSC(); // 자주하는질문, 공지 불러오기
	public ArrayList<ServiceCenterVO> listAllSC2(); // 자주하는질문, 공지 불러오기
}