package com.gxdxx.instagram.service.reply;

import com.gxdxx.instagram.dto.request.ReplyRegisterRequest;
import com.gxdxx.instagram.dto.response.ReplyRegisterResponse;
import com.gxdxx.instagram.entity.Comment;
import com.gxdxx.instagram.entity.Reply;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.CommentNotFoundException;
import com.gxdxx.instagram.exception.UserNotFoundException;
import com.gxdxx.instagram.repository.CommentRepository;
import com.gxdxx.instagram.repository.ReplyRepository;
import com.gxdxx.instagram.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@RequiredArgsConstructor
@Service
public class ReplyCreateService {

    private final ReplyRepository replyRepository;
    private final CommentRepository commentRepository;
    private final UserRepository userRepository;

    public ReplyRegisterResponse createReply(ReplyRegisterRequest request, String requestingUserNickname) {
        User registeringUser = userRepository.findByNickname(requestingUserNickname)
                .orElseThrow(UserNotFoundException::new);
        Comment registeringComment = commentRepository.findById(request.commentId())
                .orElseThrow(CommentNotFoundException::new);
        Reply registeringReply = Reply.of(request.content(), registeringUser, registeringComment);
        return ReplyRegisterResponse.of(replyRepository.save(registeringReply));
    }

}