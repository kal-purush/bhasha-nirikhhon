package com.billi.controller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.frontcontroller.CommonControllerInterface;

public class MypageController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		String page = "/";
		String method = (String)data.get("method");
		HttpServletRequest request = (HttpServletRequest) data.get("request");
		if(method.equals("GET")) {
			page = "/user/mypage.jsp";
			
		}else { }
		return page;
	}

}