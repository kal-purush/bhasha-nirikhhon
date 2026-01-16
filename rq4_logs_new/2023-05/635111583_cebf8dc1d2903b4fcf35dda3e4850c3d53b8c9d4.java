package com.example.board.controller;

import com.example.board.domain.dto.CategoryDto.SearchCategory;
import com.example.board.domain.form.CategoryForm.MergeCategory;
import com.example.board.security.TokenProvider;
import com.example.board.service.CategoryService;
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
@RequestMapping("/category")
public class CategoryController {

    private final CategoryService categoryService;
    private final TokenProvider tokenProvider;
    public final String TOKEN_HEADER = "USER_TOKEN_HEADER";


    // 조회
    @GetMapping("/search")
    public ResponseEntity<List<SearchCategory>> searchCategory() {

        return ResponseEntity.ok(categoryService.searchCategory());
    }


    // 저장
    @PostMapping("/add")
    public ResponseEntity<List<SearchCategory>> addCategory(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @RequestBody @Valid MergeCategory mergeCategory) {

        return ResponseEntity.ok(categoryService.addCategory(
            tokenProvider.getTokenUserId(token), mergeCategory));
    }


    // 수정
    @PutMapping("/modify/{categoryId}")
    public ResponseEntity<List<SearchCategory>> modifyCategory(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @PathVariable("categoryId") Long categoryId,
        @RequestBody @Valid MergeCategory mergeCategory) {

        return ResponseEntity.ok(categoryService.modifyCategory(
            tokenProvider.getTokenUserId(token), categoryId, mergeCategory));
    }


    // 삭제
    @DeleteMapping("/delete/{categoryId}")
    public ResponseEntity<List<SearchCategory>> deleteCategory(
        @RequestHeader(name = TOKEN_HEADER) String token,
        @PathVariable("categoryId") Long categoryId) {

        // 삭제 전 게시글이 있는지 확인

        return ResponseEntity.ok(categoryService.deleteCategory(
            tokenProvider.getTokenUserId(token), categoryId));
    }
}