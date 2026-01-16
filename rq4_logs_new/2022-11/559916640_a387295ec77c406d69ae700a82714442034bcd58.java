package com.project.board.controller;

import com.project.board.service.BoardService;
import com.project.board.vo.BoardVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

@Controller
@RequestMapping("/Board")
public class BoardController {

        @Autowired
        private BoardService boardService;

    @RequestMapping("/List")
    public ModelAndView list(String menu_id){

        ModelAndView mv = new ModelAndView();
        mv.setViewName("board/list");

        List<BoardVo> boardList = boardService.getBoardList(menu_id);
        mv.addObject("menu_id",menu_id);
        mv.addObject("boardList",boardList);
        return mv;
    }


}