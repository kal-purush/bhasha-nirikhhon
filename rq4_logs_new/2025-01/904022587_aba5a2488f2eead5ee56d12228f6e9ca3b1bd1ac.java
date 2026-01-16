package com.green.cs.controller;

import java.util.HashMap;
import java.util.List;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

import com.green.cs.mapper.CsMapper;
import com.green.cs.vo.CsVo;

import jakarta.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/Cs")
public class CsController {
	
	@Autowired
	private CsMapper csMapper;

	//------------------------고객센터 메인화면-----------------------------//
	// Cs/Cslist (홈)
	@RequestMapping("/Cslist")
	public ModelAndView cslist() {
		// 자주하는 질문 List
		List<CsVo> csList = csMapper.getcsList();
		System.out.println("csList=" + csList);
		
		// 공지사항 List
		List<CsVo> ntList = csMapper.getntList();
		System.out.println("ntList=" + ntList);
				
		ModelAndView mv = new ModelAndView();
		mv.addObject("csList", csList);
		mv.addObject("ntList", ntList);
		mv.setViewName("cs/cslist");
		return mv;
	}
	//------------------------고객센터 검색-----------------------------//
	// Cs/Cssearch (검색)
	@RequestMapping("/Cssearch")
	public ModelAndView cssearch(@RequestParam("searchtext") String searchtext) {
		// 자주하는 질문 List(검색어)
		List<CsVo> csList = csMapper.getsearchList(searchtext);
		System.out.println("csList=" + csList);
		
		// 검색 갯수
		int searchcount = csList.size();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("searchtext", searchtext);
		mv.addObject("searchcount", searchcount);
		mv.addObject("csList", csList);
		mv.setViewName("cs/cssearch");
		return mv;
	}
	//------------------------고객센터 자주하는질문 상세페이지-----------------------------//
	// Cs/Csview (검색)
	@RequestMapping("/Csview")
	public ModelAndView csview(@RequestParam("customer_service_idx") int customer_service_idx) {
		// 자주하는질문 상세페이지
		CsVo vo = csMapper.getfaqList(customer_service_idx);
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("vo", vo);
		mv.setViewName("cs/csview");
		return mv;
	}
	//------------------------고객센터 문의글 작성-----------------------------//
	// Cs/CswriteForm (문의글 작성)
	@RequestMapping("/CswriteForm")
	public ModelAndView cswriteForm(CsVo csVo, @RequestParam("user_idx") int user_idx) {
		System.out.println("user_idx:" + user_idx);
		
		// 회원정보
	    CsVo vo = csMapper.getminfo(user_idx);
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("csVo", csVo);
		mv.addObject("vo", vo);
		mv.addObject("user_idx", user_idx);
		mv.setViewName("cs/cswrite");
		return mv;
		}
	// Cs/Cswrite (문의글 작성)
	@RequestMapping(value = "/Cswrite", method = RequestMethod.POST)
	public ModelAndView cswrite(
	        @ModelAttribute CsVo csVo,
	        @RequestParam("user_idx") int user_idx,
	        @RequestParam("customer_service_image_name") MultipartFile[] files,
	        HttpServletRequest request) {
	    // 업로드 경로 설정
	    String uploadPath = request.getServletContext().getRealPath("/uploads");
	    HashMap<String, Object> map = new HashMap<>();
	    map.put("uploadPath", uploadPath);

	    // 파일 저장 후 경로와 파일명 저장
	    String savedPaths = FileImage.save(map, files, csVo);
	    System.out.println("Returned Saved Paths: " + savedPaths);
	    csVo.setSaved_image_paths(savedPaths); // 파일 경로 저장

	    // 문의글 저장
	    csMapper.insertcs(csVo);

	    ModelAndView mv = new ModelAndView();
		mv.addObject("user_idx", user_idx);
	    mv.setViewName("redirect:/Cs/Mycslist");
	    return mv;
	}

	//------------------------고객센터 나의 문의내역-----------------------------//
	// Cs/Mycslist (홈)
	@RequestMapping("/Mycslist")
	public ModelAndView mycslist(@RequestParam("user_idx") int user_idx) {
		
		System.out.println("user: " + user_idx);
		// 나의 문의내역 List
		List<CsVo> mycsList = csMapper.getmycsList(user_idx);
		System.out.println("mycsList=" + mycsList);
		
				
		ModelAndView mv = new ModelAndView();
		mv.addObject("mycsList", mycsList);
		mv.setViewName("cs/mycslist");
		return mv;
	}
	//------------------------고객센터 나의 문의내역 상세페이지-----------------------------//
	// Cs/Mycsview (홈)
	@RequestMapping("/Mycsview")
	public ModelAndView mycsview(@RequestParam("customer_service_idx") int customer_service_idx) {
		// 문의내역 상세페이지
		CsVo vo = csMapper.getmycs(customer_service_idx);
		
		// 문의글 답변
		CsVo aw = csMapper.getanswer(customer_service_idx);
		System.out.println("aw=" + aw);
				
		ModelAndView mv = new ModelAndView();
		mv.addObject("vo", vo);
		mv.addObject("aw", aw);
		mv.setViewName("cs/mycsview");
		return mv;
	}
	
	//------------------------공지사항 메인화면-----------------------------//
	// Cs/Noticelist (홈)
	@RequestMapping("/Noticelist")
	public ModelAndView noticelist() {
		// 공지사항 List
		List<CsVo> ntList = csMapper.getntList();
		System.out.println("ntList=" + ntList);
				
		ModelAndView mv = new ModelAndView();
		mv.addObject("ntList", ntList);
		mv.setViewName("cs/noticelist");
		return mv;
	}
	//------------------------공지사항 상세페이지-----------------------------//
	// Cs/Noticeview (공지사항 상세페이지)
	@RequestMapping("/Noticeview")
	public ModelAndView noticeview(@RequestParam("notice_idx") int notice_idx) {
		// 공지사항 상세페이지
	    CsVo vo = csMapper.getnotice(notice_idx);
	    
	    // 이전 공지사항
	    CsVo pv = csMapper.getprevious(notice_idx);
	    System.out.println("pv=" + pv);
	    
	    //  다음 공지사항
	    CsVo nt = csMapper.getnext(notice_idx);
	    System.out.println("nt=" + nt);
	    
	    ModelAndView mv = new ModelAndView();
	    mv.addObject("vo", vo);
	    mv.addObject("pv", pv);
	    mv.addObject("nt", nt);
	    mv.setViewName("cs/noticeview");
	    return mv;
	}
}