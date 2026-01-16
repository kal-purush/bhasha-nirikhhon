package com.billi.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.billi.frontcontroller.CommonControllerInterface;
import com.billi.model.BoardsService;
import com.billi.vo.BoardsVO;

public class BoardsAjaxController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		BoardsService service = new BoardsService();
		HttpServletRequest request = (HttpServletRequest) data.get("request");
		request.setCharacterEncoding("utf-8");
		List<BoardsVO> boardlist = service.selectAll();
		request.setAttribute("boardlist", boardlist);

		JSONArray arr = new JSONArray();
		for (BoardsVO board : boardlist) {
			JSONObject obj = new JSONObject();
			obj.put("board_id", board.getBoard_id());
			obj.put("board_title", board.getBoard_title());
			obj.put("address", board.getAddress());
			obj.put("price", board.getPrice());
			obj.put("board_date", board.getBoard_date());
			obj.put("board_contents", board.getBoard_contents());
			obj.put("board_writer", board.getBoard_writer());
			obj.put("category", board.getCategory());
			obj.put("pictures", board.getPictures());
			arr.add(obj);
		}
		JSONObject boardsObject = new JSONObject();
		boardsObject.put("boardlist", arr);
		return "responseBody:" + boardsObject.toJSONString();
	}
}