package com.billi.controller;

import java.sql.Date;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.frontcontroller.CommonControllerInterface;
import com.billi.model.RentalService;
import com.billi.util.DateUtil;
import com.billi.vo.RentalVO;

public class RentalController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		String method = (String)data.get("method");
		String page = "/";
		HttpServletRequest request = (HttpServletRequest) data.get("request");
		if(method.equals("GET")) {
			//
			//post
			//렌탈정보 레코드 추가
		}else { 
			RentalVO vo = makeRentalVO(request);
			RentalService rentalService = new RentalService();
			rentalService.registRental(vo);
			
			//String message = empService.updateEmp(emp);
			//page = "redirect:empList.do";
			page = "/";

		}
		
		return page;
	}

	private RentalVO makeRentalVO(HttpServletRequest request) {
		//int rental_code ; //db에서 sequence로 처리함.
		String mem_id = request.getParameter("userid");
		Date rental_date = DateUtil.convertToDate(request.getParameter("rental_date"));
		Date exp_date = DateUtil.convertToDate(request.getParameter("rental_date"));;
		int board_id = Integer.parseInt(request.getParameter("board_id")); 
		
		RentalVO vo = new RentalVO();
		//vo.setRental_code(rental_code);
		vo.setMem_id(mem_id);
		vo.setRental_date(rental_date);
		vo.setExp_date(exp_date);
		vo.setBoard_id(board_id);
		
		return vo;
	}

}