package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.dto.ResponseResult;
import com.example.EnjoyTripBackend.dto.notice.NoticeDto;
import com.example.EnjoyTripBackend.dto.notice.NoticeResponseDto;
import com.example.EnjoyTripBackend.service.NoticeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import java.util.List;

import static org.springframework.http.HttpStatus.*;

@RestController
@RequestMapping("/api/notices")
@RequiredArgsConstructor
public class NoticeController {

    private final NoticeService noticeService;

    @PostMapping("")
    public ResponseEntity<String> save(@Valid @RequestBody NoticeDto noticeDto){
        Long id = noticeService.save(noticeDto);
        return ResponseEntity.status(CREATED).body("공지사항 등록 완료");
    }

    @GetMapping("/{id}")
    public ResponseEntity<NoticeDto> findById(@PathVariable("id") Long id) {
        NoticeDto noticeDto = noticeService.findById(id);
        return ResponseEntity.status(OK).body(noticeDto);
    }

    @GetMapping("")
    public ResponseEntity<ResponseResult<List<NoticeResponseDto>>> findAll(@PageableDefault(size = 5)Pageable pageable) {
        return ResponseEntity.status(OK).body(noticeService.findAll(pageable));
    }

    @PutMapping("/{id}")
    public ResponseEntity<String> update(@PathVariable("id") Long id, @Valid @RequestBody NoticeDto noticeDto) {
        noticeService.update(id, noticeDto);
        return ResponseEntity.status(OK).body("공지사항 수정 완료");
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable("id") Long id) {
        noticeService.delete(id);
        return ResponseEntity.status(NO_CONTENT).build();
    }
}