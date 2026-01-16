package com.example.board.controller;

import com.example.board.controller.dto.BoardListResponseDto;
import com.example.board.controller.dto.BoardRequestDto;
import com.example.board.controller.dto.BoardResponseDto;
import com.example.board.repository.entity.Board;
import com.example.board.service.BoardService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;
import java.util.UUID;

@Controller
@RequestMapping("/board")
@RequiredArgsConstructor
public class BoardController {

    private final BoardService boardService;

    @GetMapping("/list")
    public String list(Model model) {
        List<Board> boards = boardService.findAll();
        List<BoardListResponseDto> boardDtos = BoardListResponseDto.fromEntityList(boards);
        model.addAttribute("boards", boardDtos);
        return "board/list";
    }

    @GetMapping("/view/{id}")
    public String view(@PathVariable("id") UUID id, Model model) {
        boardService.findById(id).ifPresent(board -> {
            BoardResponseDto boardDto = BoardResponseDto.fromEntity(board);
            model.addAttribute("board", boardDto);
        });
        return "board/view";
    }

    @GetMapping("/create")
    public String createForm(Model model) {
        model.addAttribute("boardRequest", new BoardRequestDto());
        return "board/create";
    }

    @PostMapping("/create")
    public String create(@Valid @ModelAttribute("boardRequest") BoardRequestDto boardRequestDto, 
                          BindingResult bindingResult,
                          RedirectAttributes redirectAttributes) {
        if (bindingResult.hasErrors()) {
            return "board/create";
        }
        
        Board board = boardRequestDto.toEntity();
        Board savedBoard = boardService.save(board);
        redirectAttributes.addFlashAttribute("message", "게시물이 성공적으로 등록되었습니다.");
        return "redirect:/board/view/" + savedBoard.getId();
    }

    @GetMapping("/edit/{id}")
    public String editForm(@PathVariable("id") UUID id, Model model) {
        boardService.findById(id).ifPresent(board -> {
            BoardRequestDto boardRequestDto = new BoardRequestDto();
            boardRequestDto.setTitle(board.getTitle());
            boardRequestDto.setContent(board.getContent());
            boardRequestDto.setAuthor(board.getAuthor());
            
            model.addAttribute("boardRequest", boardRequestDto);
            model.addAttribute("boardId", id);
        });
        return "board/edit";
    }

    @PostMapping("/edit/{id}")
    public String edit(@PathVariable("id") UUID id,
                       @Valid @ModelAttribute("boardRequest") BoardRequestDto boardRequestDto,
                       BindingResult bindingResult,
                       RedirectAttributes redirectAttributes) {
        if (bindingResult.hasErrors()) {
            return "board/edit";
        }
        
        boardService.findById(id).ifPresent(existingBoard -> {
            existingBoard.setTitle(boardRequestDto.getTitle());
            existingBoard.setContent(boardRequestDto.getContent());
            // 작성자는 변경하지 않음
            boardService.save(existingBoard);
            redirectAttributes.addFlashAttribute("message", "게시물이 성공적으로 수정되었습니다.");
        });
        return "redirect:/board/view/" + id;
    }

    @PostMapping("/delete/{id}")
    public String delete(@PathVariable("id") UUID id, RedirectAttributes redirectAttributes) {
        boardService.deleteById(id);
        redirectAttributes.addFlashAttribute("message", "게시물이 성공적으로 삭제되었습니다.");
        return "redirect:/board/list";
    }

    @GetMapping("/search")
    public String search(@RequestParam("type") String type, @RequestParam("keyword") String keyword, Model model) {
        List<Board> boards;
        
        if ("title".equals(type)) {
            boards = boardService.findByTitle(keyword);
        } else if ("author".equals(type)) {
            boards = boardService.findByAuthor(keyword);
        } else {
            boards = boardService.findAll();
        }
        
        List<BoardListResponseDto> boardDtos = BoardListResponseDto.fromEntityList(boards);
        model.addAttribute("boards", boardDtos);
        return "board/list";
    }
}