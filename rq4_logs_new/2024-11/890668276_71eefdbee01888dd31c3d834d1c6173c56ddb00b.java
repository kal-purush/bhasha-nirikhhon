package com.ddogdog.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ddogdog.model.dto.Board;
import com.ddogdog.model.dto.SearchCondition;
import com.ddogdog.service.BoardService;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("api")
@Tag(name="BoardRestful PI", description = "게시판 CRUD")
@CrossOrigin("*")
public class BoardController {
	// 서비스 의존성 주입
	private final BoardService boardService;
	
//	@Autowired	// 없어도 된다
	public BoardController(BoardService boardService) {
		this.boardService = boardService;
	}
	
	// 게시글 전체조회
//	@GetMapping("/board")
//	public ResponseEntity<List<Board>> list() {
//		List<Board> list = boardService.getBoardList();
//		return new ResponseEntity<>(list, HttpStatus.OK);
//	}
	
	// 게시글 검색
	@GetMapping("/board")
	@Operation(summary = "게시글 검색 및 조회", description = "조건에 따른 검색을 수행할 수 있습니다.")
	public ResponseEntity<?> list(@ModelAttribute SearchCondition condition) {
		System.out.println(condition);
		List<Board> list = boardService.search(condition);
		
		if(list == null || list.size() == 0) {
			return new ResponseEntity<Void>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<>(list, HttpStatus.OK);
	}
	
	// 게시글 상세 보기
	@GetMapping("/board/{id}")
	public ResponseEntity<Board> detail(@PathVariable("id") int id) {
		Board board = boardService.readBoard(id);
		if(board != null) {
			return ResponseEntity.ok(board);
		}
		return ResponseEntity.status(HttpStatus.NOT_FOUND).build();	// 없는 게시글 검색 시 NotFoundError를 띄워준다
	}
	
	// 게시글 등록(Form 데이터 형식으로 넘어온 경우)
	@PostMapping("/board")
	public ResponseEntity<?> write(@ModelAttribute Board board) {
		boardService.writeBoard(board);
		
		int id = board.getBoardId();
		return new ResponseEntity<Board>(board, HttpStatus.CREATED);
	}
	
	// 게시글 삭제
	@DeleteMapping("/board/{id}")
	@Hidden
	public ResponseEntity<String> delete(@PathVariable("id") int id) {
		boolean isDeleted = boardService.removeBoard(id);
		if(isDeleted)
			return ResponseEntity.status(HttpStatus.OK).body("Board deleted");
		
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed");
	}
	
	// 게시글 수정
//	@PutMapping("/board")
	@PutMapping("/board/{id}")
	public ResponseEntity<Void> update(@PathVariable("id") int id, @RequestBody Board board) {
		board.setBoardId(id);
		boardService.modifyBoard(board);
		return new ResponseEntity<Void>(HttpStatus.OK);
	}
	
}