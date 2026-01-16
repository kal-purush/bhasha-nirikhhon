package faang.school.postservice.controller;

import faang.school.postservice.dto.post.PostDto;
import faang.school.postservice.mapper.PostMapper;
import faang.school.postservice.service.PostService;
import faang.school.postservice.validation.PostDtoValidator;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RequiredArgsConstructor
@RequestMapping("/post")
@RestController
public class PostController {
    private final PostService postService;
    private final PostMapper postMapper;
    private final PostDtoValidator postDtoValidator;

    @PostMapping("/draft")
    public PostDto createDraft(@RequestBody @Valid PostDto postDto) {
        postDtoValidator.isValid(postDto, null);
        return postMapper.toDto(postService.createDraft(postMapper.toEntity(postDto)));
    }

    @PostMapping("/publish/{postId}")
    public PostDto publish(@PathVariable Long postId) {
        return postMapper.toDto(postService.publish(postId));
    }

    @PatchMapping
    public PostDto update(@RequestBody PostDto postDto) {
        return postMapper.toDto(postService.update(postMapper.toEntity(postDto)));
    }

    @DeleteMapping("/{postId}")
    public void delete(@PathVariable Long postId) {
        postService.delete(postId);
    }

    @GetMapping("/{postId}")
    public PostDto get(@PathVariable Long postId) {
        return postMapper.toDto(postService.get(postId));
    }

    @GetMapping("/draft/user/{userId}")
    public List<PostDto> getDraftsByAuthorId(@PathVariable Long userId) {
        return postMapper.toDto(postService.getDraftsByAuthorId(userId));
    }

    @GetMapping("/draft/project/{projectId}")
    public List<PostDto> getDraftsByProjectId(@PathVariable Long projectId) {
        return postMapper.toDto(postService.getDraftsByProjectId(projectId));
    }

    @GetMapping("/user/{userId}")
    public List<PostDto> getPostsByAuthorId(@PathVariable Long userId) {
        return postMapper.toDto(postService.getPostsByAuthorId(userId));
    }

    @GetMapping("/project/{projectId}")
    public List<PostDto> getPostsByProjectId(@PathVariable Long projectId) {
        return postMapper.toDto(postService.getPostsByProjectId(projectId));
    }

}