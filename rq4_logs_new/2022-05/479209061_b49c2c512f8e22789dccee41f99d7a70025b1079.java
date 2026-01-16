package com.geekmovie.board;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.geekmovie.board.service.FreeBoardService;
import com.geekmovie.board.vo.BoardVo;
import com.geekmovie.board.vo.FreeVo;
import com.geekmovie.board.vo.PageVo;

@RestController
public class FreeBoardControllerApi {

	@Autowired
	FreeBoardService fBoardService;
	
	
	@RequestMapping("/freeboardList/{userId}")          //게시판 userId가 작성한 글 반환 
	public List<FreeVo> freeboardList(FreeVo freeVo,
			@RequestParam(required = false, defaultValue = "1") int curPage,
			@RequestParam(required = false, defaultValue = "1") int range,
			@PathVariable("userId") String userId) {
		freeVo.setbKeyword(userId);
		String SearchType = "Wr";
		freeVo.setSearchType(SearchType);
		int listCnt = fBoardService.fListCnt(freeVo);
		

		PageVo pagevo = new PageVo();
		
		pagevo.pageInfo(curPage, range, listCnt);
		freeVo.setStartList(pagevo.getStartList());
		freeVo.setListSize(pagevo.getListSize());
		
		List<FreeVo> list = fBoardService.fList(freeVo);
		

		
		return list;
	}

	@RequestMapping("/freeboardListRecommend")          //추천 리뷰를 반환
	public List<FreeVo> freeboardListRecommend(FreeVo freeVo,
			@RequestParam(required = false, defaultValue = "1") int curPage,
			@RequestParam(required = false, defaultValue = "1") int range,
			@RequestParam(required = false, defaultValue = "TC") String searchType,
			@RequestParam(required = false, defaultValue = "") String bKeyword) {
		
		
		if(freeVo.getSearchType() == null) freeVo.setSearchType("TC");
		int listCnt = fBoardService.freeRecommendCnt(freeVo);
		PageVo pagevo = new PageVo();
		pagevo.pageInfo(curPage, range, listCnt);
		freeVo.setStartList(pagevo.getStartList());
		freeVo.setListSize(pagevo.getListSize());
		List<FreeVo> list = fBoardService.freeRecommend(freeVo);

		return list;
	}
}