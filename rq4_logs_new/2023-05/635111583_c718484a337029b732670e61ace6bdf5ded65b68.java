package com.example.board.controller;

import com.example.board.domain.dto.BoardDto.SearchContentBoard;
import com.example.board.domain.dto.BoardDto.SearchListBoard;
import com.example.board.domain.form.BoardForm.ListBoard;
import com.example.board.domain.form.BoardForm.MergeBoard;
import com.example.board.security.TokenProvider;
import com.example.board.service.BoardService;
import java.util.List;
import javax.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
@RequestMapping("/board")
public class BoardController {

    private final BoardService boardService;
    private final TokenProvider tokenProvider;
    public final String TOKEN_HEADER = "USER_TOKEN_HEADER";


    // 게시판 목록 조회
    @GetMapping("/list/{page}/{size}")
    public ResponseEntity<List<SearchListBoard>> searchListBoard(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @PathVariable("page") int page, @PathVariable("size") int size,
        @RequestBody @Valid ListBoard listBoard) {

        return ResponseEntity.ok(boardService.searchListBoard(
            tokenProvider.getTokenUserId(token), page, size, listBoard));
    }


    // 게시판 내용 조회
    @GetMapping("/content/{categoryId}/{boardId}")
    public ResponseEntity<SearchContentBoard> searchContentBoard(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @PathVariable("categoryId") Long categoryId,
        @PathVariable("boardId") Long boardId) {

        return ResponseEntity.ok(boardService.searchContentBoard(
            tokenProvider.getTokenUserId(token), categoryId, boardId));
    }


    // 저장
    @PostMapping("/add")
    public ResponseEntity<SearchContentBoard> addBoard(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @RequestBody @Valid MergeBoard mergeBoard) {

        return ResponseEntity.ok(boardService.addBoard(
            tokenProvider.getTokenUserId(token), mergeBoard));
    }


    // 수정
    @PutMapping("/modify/{boardId}")
    public ResponseEntity<SearchContentBoard> modifyBoard(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @PathVariable("boardId") Long boardId,
        @RequestBody @Valid MergeBoard mergeBoard) {

        return ResponseEntity.ok(boardService.modifyBoard(
            tokenProvider.getTokenUserId(token), boardId, mergeBoard));
    }


    // 삭제
    @DeleteMapping("/delete/{boardId}")
    public ResponseEntity<List<SearchListBoard>> deleteCategory(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @PathVariable("boardId") Long boardId) {

        return ResponseEntity.ok(boardService.deleteBoard(
            tokenProvider.getTokenUserId(token), boardId));
    }
}