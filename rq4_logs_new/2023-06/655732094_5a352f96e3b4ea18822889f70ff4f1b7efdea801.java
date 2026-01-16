package com.exam.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.exam.bo.QuestionBo;
import com.exam.service.QuestionService;

@Controller
public class QuestionController {
	@Autowired
	QuestionService questionService;
	
	QuestionBo questionBo=new QuestionBo();
	@RequestMapping(value="createQuestionPage",method=RequestMethod.GET)
	public String createQuestionPage(Model model){
		model.addAttribute("questionBo", questionBo);
		return "createQuestion";
	}
	@RequestMapping(value="createQuestion",method=RequestMethod.POST)
	public String createQuestion(@ModelAttribute("questionBo")QuestionBo bo,Model model){
		int id=questionService.createQuestion(bo);
		model.addAttribute("questionBo", questionBo);
		return "createQuestion";
	}


}