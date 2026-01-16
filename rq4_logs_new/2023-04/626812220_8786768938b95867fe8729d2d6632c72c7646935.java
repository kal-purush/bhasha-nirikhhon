package com.billi.frontcontroller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.model.BoardsService;
import com.billi.vo.BoardsVO;

public class BoardDetailController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		String method=(String) data.get("method");
		String page="boarddetail.jsp";
		HttpServletRequest request =(HttpServletRequest)data.get("request");
		if(method.equals("GET")) {
			int id = Integer.parseInt(request.getParameter("num"));

			BoardsService service = new BoardsService();
			BoardsVO board = service.selectById(id);
			request.setAttribute("board", board);
			

		} else {
//			//수정
//			EmpVO emp = makeEmp(request);
//			EmpService service = new EmpService();
//			String message = service.empUpdate(emp);

			page="redirect:emplist.do";
		}
		return page;
	}

}