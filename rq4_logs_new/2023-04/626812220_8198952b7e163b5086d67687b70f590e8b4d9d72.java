package com.billi.frontcontroller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.billi.model.RentalService;
import com.billi.vo.Board_RentalVO;
import com.billi.vo.MembersVO;

public class MyrentalController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		HttpSession session = request.getSession();
		
		MembersVO user_vo = (MembersVO) session.getAttribute("loginUser");
		String user_nick = user_vo.getNickname();
		
		RentalService service = new RentalService();
		List<Board_RentalVO> rentallist = service.myRental(user_nick);
		
		request.setAttribute("rentallist", rentallist);
		
		return "myrentallist.jsp";
	}

}