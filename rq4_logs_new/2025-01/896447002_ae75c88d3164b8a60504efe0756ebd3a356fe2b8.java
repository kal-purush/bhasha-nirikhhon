package com.example.musing.notice.controller;

import com.example.musing.common.dto.ResponseDto;
import com.example.musing.notice.dto.NoticeDto;
import com.example.musing.notice.service.NoticeService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/musing")
@RequiredArgsConstructor
@RestController
public class NoticeController {
    private final NoticeService noticeService;

    @GetMapping("/notice/list")
    public ResponseDto<Page<NoticeDto>> noticeList(@RequestParam(name = "page", defaultValue = "1") int page){
        Page<NoticeDto> noticeDtos = noticeService.getNoticeList(page);
        return ResponseDto.of(noticeDtos);
    }

    @GetMapping("/notice")
    public ResponseDto<NoticeDto> notice(@RequestParam int noticeId){
        NoticeDto noticeDtos = noticeService.getNotice(noticeId);
        return ResponseDto.of(noticeDtos);
    }
}