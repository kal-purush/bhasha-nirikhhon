package com.gxdxx.instagram.service.comment;

import com.gxdxx.instagram.dto.request.CommentRegisterRequest;
import com.gxdxx.instagram.dto.response.CommentRegisterResponse;
import com.gxdxx.instagram.entity.Comment;
import com.gxdxx.instagram.entity.Post;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.PostNotFoundException;
import com.gxdxx.instagram.exception.UserNotFoundException;
import com.gxdxx.instagram.repository.CommentRepository;
import com.gxdxx.instagram.repository.PostRepository;
import com.gxdxx.instagram.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@RequiredArgsConstructor
@Service
public class CommentCreateService {

    private final CommentRepository commentRepository;
    private final PostRepository postRepository;
    private final UserRepository userRepository;

    public CommentRegisterResponse createComment(CommentRegisterRequest request, String requestingUserNickname) {
        User registeringUser = userRepository.findByNickname(requestingUserNickname)
                .orElseThrow(UserNotFoundException::new);
        Post postForComment = postRepository.findById(request.postId())
                .orElseThrow(PostNotFoundException::new);
        Comment registeringComment = Comment.of(request.content(), registeringUser, postForComment);
        return CommentRegisterResponse.of(commentRepository.save(registeringComment));
    }

}