package com.gosh.boardController;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import com.gosh.boardDAO.BoardDAO;
import com.gosh.boardDTO.BoardDTO;


@RequestMapping("board")
@Controller
public class BoardController {

	@Autowired
	BoardDAO dao;
	
	@RequestMapping("board/board.do")
	public ModelAndView board(ModelAndView mav){
		List<BoardDTO> list = dao.getBoardList();
		mav.setViewName("board");
		mav.addObject("boardlist",list);
		return mav;
		
	}
	
	 @RequestMapping("board/write")
	 public String board_write (){
	     	return "board/write";
	 }
}