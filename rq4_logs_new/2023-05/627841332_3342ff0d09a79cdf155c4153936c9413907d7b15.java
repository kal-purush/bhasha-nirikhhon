package com.island.app.community.notice.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("notice")
public class MemberNoticeController {
	//공지사항리스트
		@GetMapping("list")
		public String Noticelist() {
			return "community/commu-main";
		}
		
	//공지사항 상세
		@GetMapping("detail")
		public String Noticedetail() {
			return "community/notice/commu-Noticedetail";
		}

}