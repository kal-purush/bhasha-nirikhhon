package com.iot7.controller;

import com.iot7.dto.MenuSubscribeDTO;
import com.iot7.service.MenuSubscribeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/menu-subscribe")
public class MenuSubscribeController {

    @Autowired
    private MenuSubscribeService menuSubscribeService;

    // 구독 or 구독 취소 토글 API
    @PostMapping
    public boolean toggle(@RequestBody MenuSubscribeDTO dto) {
        // 프론트에서 전달한 userId + menuId DTO 받아서 구독 상태 변경
        return menuSubscribeService.toggleSubscribe(dto);
    }

    // 구독 여부 확인 API (하트 상태용)
    @GetMapping("/check")
    public boolean isSubscribed(@RequestParam Long userId, @RequestParam Long menuId) {
        //사용자가 메뉴를 구독 중인지 여부 리턴
        return menuSubscribeService.isSubscribed(userId, menuId);
    }
}