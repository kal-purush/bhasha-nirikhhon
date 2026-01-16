package com.geekmovie.board;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.board.service.FreeBoardService;
import com.geekmovie.board.vo.FreeVo;

@RestController
public class FreeBoardApiController {
	
	@Autowired
	FreeBoardService freeBoardService;
	
	@RequestMapping(value = "/freeboard/user/{userId}", method = RequestMethod.GET)
	public List<FreeVo> UserLike(HttpServletRequest request, @PathVariable("userId") String userId) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("userId", userId);
		map.put("nowPageStart", Integer.parseInt(request.getParameter("nowPageStart")));
		map.put("Size", Integer.parseInt(request.getParameter("Size")));

		return freeBoardService.UserLike(map);
	}
}