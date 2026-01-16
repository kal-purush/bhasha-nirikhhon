package com.iot7.controller;

import com.iot7.dto.MenuRegisterRequestDTO;
import com.iot7.service.MenuManageService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/menu")
public class MenuManageController {

    private final MenuManageService menuManageService;

    public MenuManageController(MenuManageService menuManageService) {
        this.menuManageService = menuManageService;
    }

    // ✅ 메뉴 등록 API
    @PostMapping("/register/{businessId}")
    public ResponseEntity<?> registerMenu(
            @PathVariable Long businessId,
            @RequestBody MenuRegisterRequestDTO requestDTO) {
        String result = menuManageService.registerMenu(requestDTO, businessId);
        if (result.equals("메뉴 등록 성공!")) {
            return ResponseEntity.ok(Map.of("message", result));
        } else {
            return ResponseEntity.badRequest().body(Map.of("error", result));
        }
    }
}