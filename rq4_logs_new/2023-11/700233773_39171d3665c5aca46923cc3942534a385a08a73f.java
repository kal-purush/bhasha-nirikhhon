package com.example.team258.domain.elasticsearch.controller;

import com.example.team258.common.dto.BookResponseDto;
import com.example.team258.domain.elasticsearch.application.ElasticBookResponse;
import com.example.team258.domain.elasticsearch.service.ElasticBookService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@RequestMapping
@Controller
public class ElasticBookSearchController {

    private final ElasticBookService elasticBookService;

    //// 무한스크롤 기능 구현 초기 페이지 진입
    //@GetMapping("/search/el1")
    //public String elasticSearchResults(@RequestParam(value = "bookCategoryName", required = false) String bookCategoryName,
    //                                   @RequestParam(value = "keyword", required = false) String keyword,
    //                                   @RequestParam(value = "page", defaultValue = "0", required = false) Integer page,
    //                                   Model model) {
    //
    //    long startTime = System.currentTimeMillis(); // 실행시간 측정
    //
    //    // 페이지 요청 시, 현재까지의 모든 페이지를 가져오도록 수정
    //    Slice<BookResponseDto> bookResponseDtoLoadMore = searchService.getMoreBooksByCategoryOrKeyword(bookCategoryName, keyword, page);
    //
    //    model.addAttribute("categories", adminCategoriesService.getAllCategories());
    //    model.addAttribute("currentPage", page);
    //    model.addAttribute("books", bookResponseDtoLoadMore.getContent());
    //    model.addAttribute("hasNext", bookResponseDtoLoadMore.hasNext());
    //
    //    long endTime = System.currentTimeMillis();
    //    long durationTimeSec = endTime - startTime;
    //    System.out.println(durationTimeSec + "m/s"); // 실행시간 측정
    //
    //    return "users/searchIS1";
    //}

    @GetMapping("search/elastic")
    @ResponseBody
    public ResponseEntity<List<ElasticBookResponse>> search(
            @RequestParam("keyword") String book_name,
            @RequestParam(value = "page", defaultValue = "0", required = false) Integer page) {
        List<ElasticBookResponse> elasticBookResponses = elasticBookService.searchByBookName(book_name, page)
                .stream()
                .map(ElasticBookResponse::from)
                .collect(Collectors.toList());
        return ResponseEntity.ok(elasticBookResponses);
    }
}