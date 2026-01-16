package com.gxdxx.instagram.service.post;

import com.gxdxx.instagram.dto.request.PostRegisterRequest;
import com.gxdxx.instagram.dto.response.PostRegisterResponse;
import com.gxdxx.instagram.entity.Post;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.UserNotFoundException;
import com.gxdxx.instagram.repository.PostRepository;
import com.gxdxx.instagram.repository.UserRepository;
import com.gxdxx.instagram.service.S3Uploader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@RequiredArgsConstructor
@Service
public class PostCreateService {

    private final PostRepository postRepository;
    private final UserRepository userRepository;
    private final S3Uploader s3Uploader;

    public PostRegisterResponse createPost(PostRegisterRequest request, String requestingUserNickname) {
        User registeringUser = userRepository.findByNickname(requestingUserNickname)
                .orElseThrow(UserNotFoundException::new);
        String imageUrl = s3Uploader.upload(request.image(), "images");
        Post registeringPost = Post.of(request.content(), imageUrl, registeringUser);
        return PostRegisterResponse.of(postRepository.save(registeringPost));
    }

}