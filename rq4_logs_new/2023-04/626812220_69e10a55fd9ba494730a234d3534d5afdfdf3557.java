package com.billi.frontcontroller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.model.BoardsService;
import com.billi.model.ReviewsService;

public class BoarddeleteController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		int board_id = Integer.parseInt(request.getParameter("num"));
		
		BoardsService service = new BoardsService();
		service.boardDelete(board_id);
		return "redirect:boardlist.do?pageNum=1&category=all";
	}

}