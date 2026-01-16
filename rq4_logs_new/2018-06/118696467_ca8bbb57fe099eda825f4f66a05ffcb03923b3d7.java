package com.belle.teeth.api.user.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.belle.teeth.api.common.dto.Dentist;
import com.belle.teeth.api.common.dto.FileDto;
import com.belle.teeth.api.common.dto.MemberDto;
import com.belle.teeth.api.common.dto.PagingDto;
import com.belle.teeth.api.common.dto.SessionDto;
import com.belle.teeth.api.common.service.CommonService;
import com.belle.teeth.api.dentist.dto.QaDto;
import com.belle.teeth.api.dentist.service.MemberService;
import com.belle.teeth.api.dentist.service.NoticeService;
import com.belle.teeth.api.dentist.util.SessionUtil;
import com.belle.teeth.api.factory.zxing.MakeQrCode;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
@RequestMapping(value = "/user")
public class UserController {

	@Autowired
	private MemberService memberService;
	@Autowired
	private NoticeService noticeService;
	
	/**
	 * 메인 페이지
	 * @param request
	 * @param response
	 * @param model
	 * @return
	 */
	@RequestMapping(value = "/main", method = RequestMethod.GET)
	public String Main(HttpServletRequest request, HttpServletResponse response, Model model) {

		return "user/main";
	}
	
	/**
	 * 로그인 페이지
	 * @param request
	 * @param response
	 * @param model
	 * @return
	 */
	@RequestMapping(value = "/login", method = RequestMethod.GET)
	public String login(HttpServletRequest request, HttpServletResponse response, Model model) {

		return "user/login";
	}
	
	/**
	 * 공지 사항 페이지
	 * @param request
	 * @param response
	 * @param model
	 * @return
	 */
	@RequestMapping(value = "/notice", method = RequestMethod.GET)
	public String notice(HttpServletRequest request, HttpServletResponse response, Model model) {
		SessionDto sessionInfo = SessionUtil.getSessionCheck(request);
		model.addAttribute("NoticeList", noticeService.getNoticeList(sessionInfo.getDentist().getDentistNo()));
		return "user/menu/notice";
	}
	
	/**
	 * 교정 상담 페이지
	 * @param request
	 * @param response
	 * @param model
	 * @return
	 */
	@RequestMapping(value = "/qa", method = RequestMethod.GET)
	public String qa(HttpServletRequest request, HttpServletResponse response, Model model) {
		SessionDto sessionInfo = SessionUtil.getSessionCheck(request);
		model.addAttribute("qaList", memberService.getQaList(sessionInfo.getDoctorNo(), sessionInfo.getMemberNo())); // 의사번호 / 회원 번호
		model.addAttribute("memberNo", sessionInfo.getMemberNo());
		model.addAttribute("doctorNo", sessionInfo.getDoctorNo());
		return "user/menu/qa";
	}
	
	/**
	 * 질문 답변 등록
	 * @param request
	 * @param response
	 * @param message
	 * @param memberNo
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/ajax/qa/add", method = RequestMethod.POST)
	@ResponseBody
	public String QaAdd(HttpServletRequest request, HttpServletResponse response
			, @RequestParam(value="message", required=true) String message) throws Exception {
		
		QaDto qaInfo = new QaDto();
		SessionDto sessionInfo = SessionUtil.getSessionCheck(request);
		qaInfo.setType("SE02");
		qaInfo.setQaDoctorNo(sessionInfo.getDoctorNo());
		qaInfo.setQaMemberNo(sessionInfo.getMemberNo());
		qaInfo.setMessage(message);
		memberService.addQa(qaInfo);

		return "true";
	}
	

	/**
	 * 치과 방문 일정 페이지
	 * @param request
	 * @param response
	 * @param model
	 * @return
	 */
	@RequestMapping(value = "/schedual", method = RequestMethod.GET)
	public String schedual(HttpServletRequest request, HttpServletResponse response, Model model) {

		return "user/menu/schedual";
	}

}