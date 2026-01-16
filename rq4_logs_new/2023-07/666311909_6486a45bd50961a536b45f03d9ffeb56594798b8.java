package com.sparta.communityback.controller;

import com.sparta.communityback.dto.PostRequestDto;
import com.sparta.communityback.dto.PostResponseDto;
import com.sparta.communityback.entity.Post;
import com.sparta.communityback.jwt.JwtUtil;
import com.sparta.communityback.service.PostService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class PostController {

    private final PostService postService;
    private final JwtUtil jwtUtil;

    //<전체 조회하기>
    @GetMapping("/posts")
    public List<PostResponseDto> getPosts(){
        return postService.findAll();
    }
    //<게시글 작성하기>
    @PostMapping("/posts")
    public PostResponseDto createPost(@RequestBody PostRequestDto postRequestDto, HttpServletRequest req)
    {
        String token = authentication(req);
        return postService.createPost(postRequestDto, token);
    }
    private String authentication(HttpServletRequest req) {
        String tokenValue = jwtUtil.getTokenFromRequest(req);
        String token = jwtUtil.substringToken(tokenValue);

        if(!jwtUtil.validateToken(token)){
            throw new IllegalArgumentException("Token Error");
        }
        return token;
    }
    //<상세 조회하기>
    @GetMapping("/post/{id}")
    public Optional<Post> getPost(@PathVariable("id") Long id ){
        return postService.getPostById(id);
    }
    //<업데이트하기>
    @PutMapping("/post/{id}")
    public PostResponseDto updatePost(@PathVariable("id") Long id, @RequestBody PostRequestDto postrequestDto, HttpServletRequest req){
        String token = authentication(req);
        return postService.updatePost(id, postrequestDto, token);

    }
    //<삭제하기>
    @DeleteMapping("/post/{id}")
    public String deletePost(@PathVariable("id") Long id, @RequestBody PostRequestDto postRequestDto, HttpServletRequest req){
        String token = authentication(req);
        return postService.deletePost(id, postRequestDto, token);
    }

}