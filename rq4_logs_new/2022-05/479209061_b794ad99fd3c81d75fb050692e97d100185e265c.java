package com.geekmovie.board;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.board.service.BoardService;
import com.geekmovie.board.vo.BoardVo;
import com.geekmovie.board.vo.PageVo;

@RestController
public class BoardControllerApi {

	@Autowired
	BoardService boardService;
	
	@RequestMapping("/boardList/{userId}")          //게시판 userId가 작성한 글 반환 
	public List<BoardVo> boardList(BoardVo boardVo,
			@RequestParam(required = false, defaultValue = "1") int curPage,
			@RequestParam(required = false, defaultValue = "1") int range,
			@PathVariable("userId") String userId) {
		boardVo.setbKeyword(userId);
		String SearchType = "Wr";
		boardVo.setSearchType(SearchType);
		int listCnt = boardService.boardListCnt(boardVo);
		

		PageVo pagevo = new PageVo();
		
		pagevo.pageInfo(curPage, range, listCnt);
		boardVo.setStartList(pagevo.getStartList());
		boardVo.setListSize(pagevo.getListSize());
		
		List<BoardVo> list = boardService.bList(boardVo);
		

		
		return list;
	}
	
	
}