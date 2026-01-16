package org.springbootdeveloper2.controller;

import lombok.RequiredArgsConstructor;
import org.springbootdeveloper2.domain.Article;
import org.springbootdeveloper2.dto.request.AddArticleRequest;
import org.springbootdeveloper2.dto.response.ArticleResponse;
import org.springbootdeveloper2.dto.request.UpdateArticleRequest;
import org.springbootdeveloper2.service.BlogService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequiredArgsConstructor
@RestController // 객체 데이터를 JSON 형식으로 반환
public class BlogApiController {

    private final BlogService blogService; // 컨트롤러는 서비스, 서비스는 레포지토리 주입

    // HTTP 메서드가 POST일 때 전달받은 URL과 동일하면 메서드로 매핑
    // 블로그 글 저장
    @PostMapping("/api/articles")
    // @RequestBody로 요청 본문 값 매핑
    public ResponseEntity<Article> addArticle(@RequestBody AddArticleRequest addArticleRequest) {
        // DTO로 클라이언트의 요청을 받아 save 후 savedArticle 객체에 담은 후 바디에 리턴
        Article savedArticle = blogService.save(addArticleRequest);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(savedArticle);
    }

    // 블로그 글 전체 목록 조회
    @GetMapping("/api/articles")
    public ResponseEntity<List<ArticleResponse>> findAllArticles() {
        List<ArticleResponse> articles = blogService.findAll()
                .stream()
                .map(ArticleResponse::new)
                .toList();
        return ResponseEntity.ok()
                .body(articles);
    }

    // 블로그 글 한 개 조회
    @GetMapping("/api/articles/{id}")
    // URL 경로에서 값 추출
    public ResponseEntity<ArticleResponse> findArticle(@PathVariable long id) {
        Article article = blogService.findById(id);
        return ResponseEntity.ok()
                .body(new ArticleResponse(article));
    }

    // 블로그 글 한 개 삭제
    @DeleteMapping("/api/articles/{id}")
    public ResponseEntity<Void> deleteArticle(@PathVariable Long id) {
        blogService.delete(id);
        return ResponseEntity.ok().build();
    }

    // 블로그 글 수정
    @PutMapping("/api/articles/{id}")
    public ResponseEntity<Article> updateArticles(@PathVariable Long id,
                                                  @RequestBody UpdateArticleRequest updateArticleRequest) {
        Article updatedArticle = blogService.update(id, updateArticleRequest);
        return ResponseEntity.ok()
                .body(updatedArticle);
    }

}