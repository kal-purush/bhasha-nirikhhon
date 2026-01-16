package com.billi.controller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.billi.frontcontroller.CommonControllerInterface;
import com.billi.model.MembersService;
import com.billi.vo.MembersVO;

public class SignUpController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		String page="";
		String method = (String)data.get("method");
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		System.out.println(request);
		
		if(method.equals("GET")) {
			page = "signup.jsp";
		}else { //db에 유저 정보 저장
			MembersVO members = makeMembers(request);
			MembersService service = new MembersService();
			int result = service.membersInsert(members);
			String path = request.getContextPath();
			page = "redirect:"+ path + "/auth/loginCheck.do";
		}
		return page;
	}

	private MembersVO makeMembers(HttpServletRequest request) {
		String mem_id = request.getParameter("mem_id");
		String pw = request.getParameter("pw");
		String mem_name = request.getParameter("mem_name");
		String phone = request.getParameter("phone");
		String address = request.getParameter("address");
		String nickname = request.getParameter("nickname");
		String grade = request.getParameter("grade");
		
		MembersVO members = new MembersVO();
		members.setMem_id(mem_id);
		members.setPw(pw);
		members.setMem_name(mem_name);
		members.setPhone(phone);
		members.setAddress(address);
		members.setNickname(nickname);
		members.setGrade(grade);
		
		return members;
	}

}