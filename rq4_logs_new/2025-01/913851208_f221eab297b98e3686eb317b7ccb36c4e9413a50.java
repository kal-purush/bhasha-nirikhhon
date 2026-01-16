package com.project_merge.jigu_travel.board.controller;

import com.project_merge.jigu_travel.board.dto.responseDto.BoardResponseDto;
import com.project_merge.jigu_travel.global.common.BaseResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/board")
public class BoardController {

    @GetMapping("/{pageNo}")
    @ResponseBody
    public ResponseEntity<BaseResponse<BoardResponseDto>> getAllBoard() {

        return null;
    }
}