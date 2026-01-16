package TT.tripTogether.controller;

import TT.tripTogether.config.exception.BaseResponse;
import TT.tripTogether.domain.post.dto.GetPostsRes;
import TT.tripTogether.service.PostService;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/post")
public class PostController {
    private final PostService postService;

    @GetMapping("")
    @ApiOperation("게시물 보기")
    public BaseResponse<List<GetPostsRes>> getPosts(){
        return new BaseResponse<List<GetPostsRes>>(postService.getAll());
    }

}