package com.j30n.stoblyx.adapter.in.web.controller;

import com.j30n.stoblyx.common.response.ApiResponse;
import com.j30n.stoblyx.adapter.in.web.dto.content.ContentResponse;
import com.j30n.stoblyx.application.service.content.ContentService;
import com.j30n.stoblyx.application.service.content.ContentGenerationService;
import com.j30n.stoblyx.domain.repository.QuoteRepository;
import com.j30n.stoblyx.infrastructure.security.UserPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/contents")
@RequiredArgsConstructor
public class ContentController {

    private final ContentService contentService;
    private final ContentGenerationService contentGenerationService;
    private final QuoteRepository quoteRepository;

    /**
     * 트렌딩 콘텐츠 목록을 조회합니다.
     */
    @GetMapping("/trending")
    public ResponseEntity<ApiResponse<Page<ContentResponse>>> getTrendingContents(
        @PageableDefault(size = 20) Pageable pageable
    ) {
        return ResponseEntity.ok(
            ApiResponse.success("트렌딩 콘텐츠 목록입니다.",
                contentService.getTrendingContents(pageable))
        );
    }

    /**
     * 추천 콘텐츠 목록을 조회합니다.
     */
    @GetMapping("/recommended")
    public ResponseEntity<ApiResponse<Page<ContentResponse>>> getRecommendedContents(
        @AuthenticationPrincipal UserPrincipal user,
        @PageableDefault(size = 20) Pageable pageable
    ) {
        return ResponseEntity.ok(
            ApiResponse.success("추천 콘텐츠 목록입니다.",
                contentService.getRecommendedContents(user.getId(), pageable))
        );
    }

    /**
     * 특정 책의 콘텐츠 목록을 조회합니다.
     */
    @GetMapping("/books/{bookId}")
    public ResponseEntity<ApiResponse<Page<ContentResponse>>> getContentsByBook(
        @PathVariable Long bookId,
        @PageableDefault(size = 20) Pageable pageable
    ) {
        return ResponseEntity.ok(
            ApiResponse.success("책 관련 콘텐츠 목록입니다.",
                contentService.getContentsByBook(bookId, pageable))
        );
    }

    /**
     * 콘텐츠를 검색합니다.
     */
    @GetMapping("/search")
    public ResponseEntity<ApiResponse<Page<ContentResponse>>> searchContents(
        @RequestParam String keyword,
        @PageableDefault(size = 20) Pageable pageable
    ) {
        return ResponseEntity.ok(
            ApiResponse.success("검색 결과입니다.",
                contentService.searchContents(keyword, pageable))
        );
    }

    /**
     * 콘텐츠 상세 정보를 조회합니다.
     */
    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<ContentResponse>> getContent(
        @PathVariable Long id,
        @AuthenticationPrincipal UserPrincipal user
    ) {
        ContentResponse content = contentService.getContent(id);
        if (user != null) {
            contentService.incrementViewCount(id);
        }
        return ResponseEntity.ok(
            ApiResponse.success("콘텐츠 상세 정보입니다.", content)
        );
    }

    /**
     * 콘텐츠에 좋아요를 토글합니다.
     */
    @PostMapping("/{id}/like")
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<ApiResponse<Void>> toggleLike(
        @PathVariable Long id,
        @AuthenticationPrincipal UserPrincipal user
    ) {
        contentService.toggleLike(user.getId(), id);
        return ResponseEntity.ok(ApiResponse.success("좋아요를 토글했습니다.", null));
    }

    /**
     * 콘텐츠를 북마크에 추가/제거합니다.
     */
    @PostMapping("/{id}/bookmark")
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<ApiResponse<Void>> toggleBookmark(
        @PathVariable Long id,
        @AuthenticationPrincipal UserPrincipal user
    ) {
        contentService.toggleBookmark(user.getId(), id);
        return ResponseEntity.ok(ApiResponse.success("북마크를 토글했습니다.", null));
    }

    /**
     * 문구로부터 새로운 동영상 콘텐츠를 생성합니다.
     */
    @PostMapping("/quotes/{quoteId}")
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<ApiResponse<ContentResponse>> generateContent(
        @PathVariable Long quoteId
    ) {
        return ResponseEntity.ok(
            ApiResponse.success("콘텐츠가 생성되었습니다.",
                contentService.generateContent(quoteId))
        );
    }
}