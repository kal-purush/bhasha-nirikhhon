package com.example.demo.mapapi.api;

import com.example.demo.mapapi.dto.AdministrativeCodeDTO;
import com.example.demo.mapapi.service.AdministrativeCodeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/map")
@CrossOrigin
public class MapController {

    private final AdministrativeCodeService adminService;

    // 구 선택 시 동 목록 요청 처리
    @GetMapping(path = "/{gu}", produces = "application/json; charset=UTF-8")
    public List<AdministrativeCodeDTO> getDongList(@PathVariable String gu) {
        return adminService.getDongList(gu);
    }
}