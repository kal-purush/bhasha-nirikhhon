package com.example.demo.mapapi.api;

import com.example.demo.mapapi.service.CctvService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/cctv")
@CrossOrigin
public class CctvComtroller {

    private final CctvService cctvService;

    @GetMapping(path = "/{gu}/{dong}/{pageNum}", produces = "application/json; charset=UTF-8")
    public ResponseEntity<?> getCctvs(@PathVariable String gu, @PathVariable String dong, @PathVariable int pageNum) {
        return ResponseEntity.ok().body(cctvService.getCctvs(gu, dong, pageNum));
    }

    @GetMapping("/count/{gu}/{dong}")
    public ResponseEntity<?> getCctvTotalCount(@PathVariable String gu, @PathVariable String dong) {
                return ResponseEntity.ok().body(cctvService.getTotalCount(gu, dong));
    }

}