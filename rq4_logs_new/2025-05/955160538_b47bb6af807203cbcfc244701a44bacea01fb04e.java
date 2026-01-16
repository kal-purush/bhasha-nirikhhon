package com.iot7.controller;

import com.iot7.entity.MenuClickLog;
import com.iot7.repository.MenuClickLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;


@RestController
@RequestMapping("/click")
@RequiredArgsConstructor
public class MemuClickLogController {
    private final MenuClickLogRepository clickLogRepository;

    // ✅ 메뉴 클릭 시 로그 저장
    @PostMapping("/log")
    public ResponseEntity<String> logClick(@RequestParam Long menuId) {
        MenuClickLog log = new MenuClickLog();
        log.setMenuId(menuId);
        log.setClickTime(LocalDateTime.now());

        clickLogRepository.save(log);

        return ResponseEntity.ok("✅ 메뉴 클릭 로그 저장 완료!");
    }
}