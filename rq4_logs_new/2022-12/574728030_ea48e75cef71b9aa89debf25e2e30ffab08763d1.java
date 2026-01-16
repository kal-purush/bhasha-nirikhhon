package com.golfzon.social.board.controller;

import com.golfzon.social.board.dto.BoardDto;
import com.golfzon.social.board.service.BoardService;
import com.golfzon.social.security.UserDetailsImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/meeting")
public class BoardController {

    private final BoardService boardService;

    //게시글 등록하기
    @PostMapping(value = "/{meetingId}/post-board")
    public ResponseEntity<String> postBoard(@PathVariable(name = "meetingId") Long meetingId, @RequestBody BoardDto.Request request,
                                            @AuthenticationPrincipal UserDetailsImpl userDetails) {

        return ResponseEntity.ok()
                .contentType(new MediaType("application", "json", StandardCharsets.UTF_8))
                .body(boardService.postBoard(meetingId, request, userDetails.getMember()));
    }

    //게시글 조회
    @GetMapping(value = "/{meetingId}/boards")
    public ResponseEntity<BoardDto.Response> getBoards(@PathVariable(name = "meetingId") Long meetingId,
                                                  @AuthenticationPrincipal UserDetailsImpl userDetails) {

        return ResponseEntity.ok()
                .body(boardService.getBoards(meetingId, userDetails.getMember()));
    }

    //게시글 상세보기
    @GetMapping(value = "/board/{boardId}")
    public ResponseEntity<BoardDto.DetailResponse> getBoardDetail(@PathVariable(name = "boardId") Long boardId,
                                                       @AuthenticationPrincipal UserDetailsImpl userDetails) {

        return ResponseEntity.ok()
                .body(boardService.getBoardDetail(boardId, userDetails.getMember()));
    }

    //게시글 수정하기
    @PutMapping(value = "/board/{boardId}")
    public ResponseEntity<String> updateBoard(@PathVariable(name = "boardId") Long boardId,
                                                                  @RequestBody BoardDto.Request request,
                                                                  @AuthenticationPrincipal UserDetailsImpl userDetails) {

        return ResponseEntity.ok()
                .body(boardService.updateBoard(boardId, request, userDetails.getMember()));
    }

    //게시글 삭제하기
    @DeleteMapping(value = "/board/{boardId}")
    public ResponseEntity<String> deleteBoard(@PathVariable(name = "boardId") Long boardId,
                                                @AuthenticationPrincipal UserDetailsImpl userDetails) {

        return ResponseEntity.ok()
                .contentType(new MediaType("application", "json", StandardCharsets.UTF_8))
                .body(boardService.deleteBoard(boardId, userDetails.getMember()));
    }
}