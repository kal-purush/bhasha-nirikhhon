package com.example.musing.reply.controller;

import com.example.musing.common.dto.ResponseDto;
import com.example.musing.reply.dto.ReplyDto;
import com.example.musing.reply.service.ReplyService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/musing")
@RequiredArgsConstructor
@RestController
public class ReplyController {

    private final ReplyService replyService;

    @GetMapping("/reply/myReply")
    public ResponseDto<ReplyDto> findMyReply(@RequestParam("boardId") long boardId) {
        if (checkUserEmail()) {
            ReplyDto replyDto = replyService.findMyReply(boardId);
            if(replyDto == null){
                return ResponseDto.of(null,"작성한 Reply이 없습니다.");
            }
            return ResponseDto.of(replyDto);
        }
        return ResponseDto.of(null,"로그인 이후 작성 가능합니다.");
    }


    private boolean checkUserEmail() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null || authentication.getName().equals("anonymousUser")) {
            return false;
        }

        return true;
    }

}