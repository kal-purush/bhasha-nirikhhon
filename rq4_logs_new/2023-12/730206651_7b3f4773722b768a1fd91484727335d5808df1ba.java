package com.team2.lb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

import com.team2.lb.service.MemberService;
import com.team2.lb.vo.Member;


@ControllerAdvice(annotations = Controller.class)
public class CommonController {
	
	@Autowired
	MemberService service;

	@ModelAttribute
	public void addCommonAttributes(Model model, @AuthenticationPrincipal UserDetails user) {
    if (user != null) {
        // 사용자가 로그인한 경우, member 모델을 생성하고 추가합니다.
        Member member = service.selectUser(user.getUsername());
        model.addAttribute("member", member);
    }
    // 다른 공통 데이터를 여기에 추가할 수도 있습니다.
}

}