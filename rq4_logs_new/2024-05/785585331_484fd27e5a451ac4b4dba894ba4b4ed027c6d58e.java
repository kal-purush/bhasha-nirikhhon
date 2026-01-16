package com.yeogi.infoseccert.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class DetailController {

    @GetMapping("/detail-mvc")
    public String detailMvc() {
        return "detail-template"; // 반환할 HTML 파일의 이름 (확장자 없이)
    }
}