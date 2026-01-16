package com.example.youtubeClone.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

// 개발할 때 편의를 위해서 사용하는 컨트롤러입니다.
// 개발이 완료되면 제거해주세요
@Controller
public class MainController {

    @GetMapping("/")
    public String mainForm() {
        return "main";
    }

    @GetMapping("/search")
    public String searchForm() {
        return "/Page/searchpage";
    }

    @GetMapping("/player")
    public String playerForm() {
        return "/Page/playerPage";
    }
}