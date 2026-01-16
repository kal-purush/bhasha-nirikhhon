package com.billi.frontcontroller;

import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.billi.vo.MembersVO;

public class LogoutController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		ServletContext app = request.getServletContext();
		HttpSession session = request.getSession();
		
		Object obj = app.getAttribute("userList");
		MembersVO admin = (MembersVO)session.getAttribute("loginUser");
		List<MembersVO> userList = null;
		
		if(obj!=null) {
			userList = (List<MembersVO>)obj;
			userList.remove(admin);
			app.setAttribute("userList", userList);
		}
		
		request.getSession(false).invalidate();
		
		return "responseBody:";
	}

}