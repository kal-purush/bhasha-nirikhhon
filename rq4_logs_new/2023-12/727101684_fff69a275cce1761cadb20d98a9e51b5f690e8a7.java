package com.example.demo.boardapi.api;

import com.example.demo.boardapi.dto.BoardDetailResponseDTO;
import com.example.demo.boardapi.dto.BoardResponseDTO;
import com.example.demo.boardapi.service.BoardService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/api/onelife-board")
@CrossOrigin
public class BoardController {

    private final BoardService boardService;


    // 카테고리별 게시글 목록 요청 처리
    @GetMapping("/{category}")
    public ResponseEntity<?> getBoardList(@PathVariable String category){
        log.info("/api/onelife-board/{} GET - board List Request", category);
        List<BoardResponseDTO> boardList = boardService.getBoardList(category);


        return ResponseEntity.ok().body(boardList);
    }

    // 게시글 상세보기 요청 처리
    @GetMapping("/{category}/{id}")
    public ResponseEntity<?> getBoard(@PathVariable String category, @PathVariable int id){
        log.info("/api/onelife-board/{}/{} GET - board Detail Request", category, id);
        BoardDetailResponseDTO responseDTO = boardService.getBoard(category, id);
        return ResponseEntity.ok().body(responseDTO);
    }

    // 게시글 추가
    @PostMapping("/{category}")
    public ResponseEntity<?> registerBoard(@PathVariable String category){
        log.info("/api/onelife-board/{} POST - board Register Request", category);
        boardService.registerBoard(category);

        return ResponseEntity.ok().body();

    }


    // 게시글 수정

    // 게시글 삭제



}