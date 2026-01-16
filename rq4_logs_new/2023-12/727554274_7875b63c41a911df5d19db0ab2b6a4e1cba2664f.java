package com.ll.medium.domain.post.post.controller;

import com.ll.medium.domain.member.member.entity.Member;
import com.ll.medium.domain.member.member.service.MemberService;
import com.ll.medium.domain.post.post.entiry.Post;
import com.ll.medium.domain.post.post.entiry.PostForm;
import com.ll.medium.domain.post.post.service.PostService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.server.ResponseStatusException;

import java.security.Principal;
import java.util.List;

@Controller
@RequiredArgsConstructor
public class PostController {

    private final PostService postService;
    private final MemberService memberService;

    @PreAuthorize("isAuthenticated()")
    @GetMapping("/post/write")
    public String writeForm(PostForm postForm) {
        return "post/post_form";
    }

    @PreAuthorize("isAuthenticated()")
    @PostMapping("/post/write")
    public String write(@Valid PostForm postForm, BindingResult bindingResult, Principal principal) {
        if(bindingResult.hasErrors()) {
            return "post/post_form";
        }

        Member member = memberService.getMember(principal.getName());
        postService.save(postForm, member);

        return "redirect:/";
    }

    @GetMapping("/post/{id}")
    public String detail(@PathVariable Integer id, Model model) {
        Post post = postService.getPost(id);
        model.addAttribute("post", post);

        return "post/detail";
    }

    @GetMapping("/post/list")
    public String list(Model model) {
        List<Post> posts = postService.getPublished();
        model.addAttribute("posts", posts);
        return "post/list";
    }

    @GetMapping("/b/{username}")
    public String list(@PathVariable String username, Model model) {
        List<Post> posts = postService.getPostGroupByMember(username);
        model.addAttribute("posts", posts);
        return "post/list";
    }

    @GetMapping("/b/{username}/{id}")
    public String detail(@PathVariable("username") String username, @PathVariable("id") Integer id, Model model) {
        Post post = postService.getPost(username, id);
        model.addAttribute("post", post);
        return "post/detail";
    }

    @PreAuthorize("isAuthenticated()")
    @GetMapping("/post/myList")
    public String list(Model model, Principal principal) {
        Member member = memberService.getMember(principal.getName());
        List<Post> posts = postService.getPostByUsername(member.getUsername());
        model.addAttribute("posts", posts);
        return "post/list";
    }

    @PreAuthorize("isAuthenticated()")
    @GetMapping("/post/{id}/modify")
    public String modifyForm(@PathVariable("id") Integer id, Principal principal, PostForm postForm) {
        Post post = postService.getPost(id);
        if(!post.getAuthor().getUsername().equals(principal.getName())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "수정권한이 없습니다.");
        }
        postForm.setTitle(post.getTitle());
        postForm.setBody(post.getBody());
        return "post/post_form";
    }

    @PreAuthorize("isAuthenticated()")
    @PostMapping("/post/{id}/modify")
    public String modify(@PathVariable("id") Integer id, @Valid PostForm postForm, BindingResult bindingResult, Principal principal) {
        Post post = postService.getPost(id);
        if(!post.getAuthor().getUsername().equals(principal.getName())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "수정권한이 없습니다.");
        }
        if(bindingResult.hasErrors()) {
            return "post/post_form";
        }
        postService.modify(post, postForm);

        return "redirect:/post/%s".formatted(id);
    }

    @PreAuthorize("isAuthenticated()")
    @GetMapping("/post/{id}/delete")
    public String delete(@PathVariable("id") Integer id, Principal principal) {
        Post post = postService.getPost(id);
        if(!post.getAuthor().getUsername().equals(principal.getName())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "삭제권한이 없습니다.");
        }

        postService.delete(post);

        return "redirect:/post/myList".formatted(id);
    }
}