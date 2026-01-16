package com.example.musing.report.controller;

import com.example.musing.common.dto.ResponseDto;
import com.example.musing.report.dto.ReportRequestDto;
import com.example.musing.report.service.ReportService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/musing")
@RequiredArgsConstructor
@RestController
public class ReportController {

    private final ReportService reportService;

    @GetMapping("/report/reply")
    public ResponseDto<String> reportReply(ReportRequestDto.ReportReplyRequestDto reportReplyRequestDto){
        reportService.reportReply(reportReplyRequestDto);
        return ResponseDto.of("","댓글을 신고했습니다.");
    }

    @GetMapping("/report/board")
    public ResponseDto<String> reportReply(ReportRequestDto.ReportBoardRequestDto reportBoardRequestDto){
        reportService.reportBoard(reportBoardRequestDto);
        return ResponseDto.of("","게시글을 신고했습니다.");
    }
}