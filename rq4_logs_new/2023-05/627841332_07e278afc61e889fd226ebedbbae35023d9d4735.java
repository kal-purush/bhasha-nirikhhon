package com.island.app.admin.faq.controller;

import java.util.List;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.island.app.admin.faq.service.FaqService;
import com.island.app.admin.faq.vo.FaqVo;
import com.island.app.member.vo.MemberVo;

/**
 * 
 * @author 김수경
 *
 */

@Controller
@RequestMapping("admin")
public class FaqController {
	
	private final FaqService fs;
	
	@Autowired
	public FaqController(FaqService fs) {
		this.fs = fs;
	}
	
	//Faq목록조회
	@GetMapping("faq/list")
	//public String faqList(Model model, @RequestParam(defaultValue = "1") int page) {
	public String faqList() {
		
		//서비스 
		List<FaqVo> fvoList = fs.getFaqList();
//		int listCount = fs.getFaqListCnt();
//		int currentPage = page;
//		int pageLimit = 5; 
//		int boardLimit = 10; 
		
		//화면
		//model.addAttribute("nvoList", nvoList);
		return "admin/faq-list";
	}
	
	//Faq작성하기 화면(관리자만)
	@GetMapping("faq/write")
	public String faqWrite(HttpSession session , Model model) {
		MemberVo loginMember = (MemberVo) session.getAttribute("loginMember");
		if(loginMember == null) {
			model.addAttribute("errorMsg" , "잘못된 접근입니다.");
			return "common/error-page";
		}
		String id = loginMember.getId();
		boolean isAdmin = "ADMIN".equals(id);
		
		if(!isAdmin) {
			model.addAttribute("errorMsg" , "잘못된 접근입니다.");
			return "common/error-page";
		}
		return "admin/faq-write";
	}
	
	//faq 작성하기 (관리자만)
	public String faqWrite(FaqVo vo ,  HttpSession session) {
		int result = fs.faqWrite(vo);
		
		if(result == 1 ) {
			session.setAttribute("alertMsg", "Faq 작성완료!!");
		}else {
			session.setAttribute("alertMsg", "Faq 작성 실패..");
		}
		return "redirect:/admin/faq/list";
	}
	
	
	//Faq상세조회
	@GetMapping("faq/detail")
	public String faqDetail(String num, Model model) throws Exception {
		
		FaqVo vo = fs.getFaq(num);
		if(vo == null) {
			model.addAttribute("errorMsg" , "조회실패..");
			return "common/error-page";
		}
		model.addAttribute("vo" , vo);
		return "admin/faq-detail";
		
		
	}
	
	
	//Faq수정하기 (관리자만)
	@PostMapping ("faq/edit")
	public String faqEdit(FaqVo vo , Model model , HttpSession session) {
		int result = fs.edit(vo);
		
		if(result != 1) {
			model.addAttribute("alertMsg" , "수정실패..");
			return "common/error-page";
		}
		
		session.setAttribute("alertMsg", "수정성공!!");
		return "redirect:/admin/faq/detail?num =" +vo.getNo();
		
	}
	
	//Faq 삭제하기 (관리자만)
	@GetMapping("delete")
	public String delete(String num) throws Exception {
		int result = fs.delete(num);
		
		if(result != 1) {
			throw new Exception("공지사항 삭제 실패 ...");
		}
		
		return "redirect:/notice/list?page=1";
	}
		
	
	
}