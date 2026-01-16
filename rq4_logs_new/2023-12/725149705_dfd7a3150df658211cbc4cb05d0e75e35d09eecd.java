package com.spring.boot.controller;

import com.spring.boot.MemberOption;
import com.spring.boot.dto.MemberDTO;
import com.spring.boot.service.MemberService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@Controller
public class MemberController {

    @Autowired
    private MemberService memberService;


    @RequestMapping(value = "/memberManager")
    public String memberManager(HttpServletRequest request) throws Exception{
        List<MemberDTO> members = memberService.findAll();
        request.setAttribute("members", members);
        return "memberManager";
    }

    @RequestMapping(value = "/memberDeleteAction")
    public String memberDeleteAction(@RequestParam("number") int number, HttpServletRequest request) throws Exception{
        memberService.delete(number);
        return memberManager(request);
    }
    @RequestMapping(value = "/memberRegisterAction")
    public String memberRegisterAction(MemberDTO member, HttpServletRequest request) throws Exception{
        memberService.save(member);
        return memberManager(request);
    }
    @RequestMapping(value = "/memberSearch")
    public String memberSearch(String searchDivide, String searchContent, HttpServletRequest request) throws Exception{
        if (searchContent.isEmpty()) { return memberManager(request); }

        MemberOption memberOption = MemberOption.from(searchDivide);
        List<MemberDTO> members = memberService.findByOption(memberOption, searchContent);
        request.setAttribute("members", members);

        return "memberManager";
    }


}