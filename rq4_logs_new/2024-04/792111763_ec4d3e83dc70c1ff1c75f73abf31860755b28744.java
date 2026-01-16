package com.hana.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

// 컨텐츠 종류별 컨텐츠 조회 컨트롤러
@Controller
@RequestMapping("/contents")
public class ContentsController {

    // 공간시설
    @RequestMapping("/gongan")
    public String gongan(Model model) {
        model.addAttribute("currentDiv", "컨텐츠별");
        model.addAttribute("menu", "공간시설");
        model.addAttribute("center", "cardview");
        return "index";
    }

    // 교육강좌
    @RequestMapping("/edulec")
    public String edulec(Model model) {
        model.addAttribute("currentDiv", "컨텐츠별");
        model.addAttribute("menu", "교육강좌");
        model.addAttribute("center", "cardview");
        return "index";
    }

    // 문화체험
    @RequestMapping("/culexper")
    public String culexper(Model model) {
        model.addAttribute("currentDiv", "컨텐츠별");
        model.addAttribute("menu", "문화체험");
        model.addAttribute("center", "cardview");
        return "index";
    }

    // 진료복지
    @RequestMapping("/medical")
    public String medical(Model model) {
        model.addAttribute("currentDiv", "컨텐츠별");
        model.addAttribute("menu", "진료복지");
        model.addAttribute("center", "cardview");
        return "index";
    }

    // 체육시설
    @RequestMapping("/sportsfacilities")
    public String sportsfacilities(Model model) {
        model.addAttribute("currentDiv", "컨텐츠별");
        model.addAttribute("menu", "체육시설");
        model.addAttribute("center", "cardview");
        return "index";
    }
}