package com.example.oneulnail.domain.post.service;

import com.example.oneulnail.domain.post.dto.request.PostRegisterReqDto;
import com.example.oneulnail.domain.post.dto.response.PostRegisterResDto;
import com.example.oneulnail.domain.post.entity.Post;
import com.example.oneulnail.domain.post.mapper.PostMapper;
import com.example.oneulnail.domain.post.repository.PostRepository;
import com.example.oneulnail.domain.shop.entity.Shop;
import com.example.oneulnail.domain.shop.service.ShopService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PostService {

    private final PostRepository postRepository;
    private final ShopService shopService;
    private final PostMapper postMapper;

    public PostRegisterResDto register(PostRegisterReqDto postRegisterReqDto) {
        Shop foundShop = shopService.findById(postRegisterReqDto.getShopId());
        Post newPost = buildPost(postRegisterReqDto, foundShop);

        return saveAndReturnResponse(newPost);
    }

    private Post buildPost(PostRegisterReqDto postRegisterReqDto, Shop shop) {
        return Post.builder()
                .shop(shop)
                .name(postRegisterReqDto.getName())
                .likeCount(0)
                .imgUrl(postRegisterReqDto.getImgUrl())
                .price(postRegisterReqDto.getPrice())
                .content(postRegisterReqDto.getContent())
                .category(postRegisterReqDto.getCategory())
                .build();
    }

    private PostRegisterResDto saveAndReturnResponse(Post post) {
        Post registeredPost = postRepository.save(post);
        return postMapper.PostRegisterEntityToDto(registeredPost);
    }
}