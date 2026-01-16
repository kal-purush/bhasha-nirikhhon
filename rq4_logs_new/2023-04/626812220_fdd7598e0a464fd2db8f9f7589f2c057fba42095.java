package com.billi.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.frontcontroller.CommonControllerInterface;
import com.billi.model.BoardsService;
import com.billi.vo.BoardsVO;

public class HomeController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		HttpServletRequest request = (HttpServletRequest)data.get("request");
		
		BoardsService service = new BoardsService();
		
		//인기 게시물
		List<BoardsVO> hitsList = service.getHitsList();
		request.setAttribute("hitsList", hitsList);
		
		//최근 게시물
		List<BoardsVO> latestList = service.getLatestList();
		request.setAttribute("latestList", latestList);
		
		return "index.jsp";
	}

}