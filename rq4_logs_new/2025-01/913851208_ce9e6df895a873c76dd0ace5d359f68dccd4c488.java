package com.project_merge.jigu_travel.web.ai_guide.controller;

import com.project_merge.jigu_travel.api.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller  // HTML 페이지를 반환하는 컨트롤러
@RequiredArgsConstructor
@RequestMapping("/ai-guide")
public class AiGuideController {

    private final UserService userService;
    // ai-guide.html을 반환하는 GET 요청
    @GetMapping("")
    public String aiGuidePage(Model model) {
        // 초기 데이터나 설명을 설정하여 ai-guide.html에 전달할 수 있습니다.


        return "ai_guide";  // ai-guide.html 페이지를 반환
    }

//    // POST 요청을 받아서 AI 응답을 표시
//    @GetMapping("/response")
//    public String getAiGuideResponse(Model model) {
//        // 예시로 REST API 요청을 보내고 응답을 받아 처리하는 코드
//        AiGuideResponse response = // FastApiClient를 통해 얻은 AI 응답
//
//                model.addAttribute("answer", response.getAnswer());
//        model.addAttribute("conversationHistory", response.getConversation_history().getHistory());
//
//        return "ai_guide_response";  // ai_guide_response.html로 응답
//    }
}