package com.billi.controller;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import com.billi.frontcontroller.CommonControllerInterface;
import com.billi.model.RentalManageService;

public class RentalRejectController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		String method = (String)data.get("method");
		String page = "myrental.do";
		HttpServletRequest request = (HttpServletRequest) data.get("request");
		
		RentalManageService service = new RentalManageService();
		int rental_code = Integer.parseInt(request.getParameter("rental_code"));
		service.rejectRental(rental_code);
		
		return page;
	}

}