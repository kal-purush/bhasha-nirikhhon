package com.spartanullnull.otil.domain.like.service;

import com.spartanullnull.otil.domain.comment.entity.Comment;
import com.spartanullnull.otil.domain.comment.repository.CommentRepository;
import com.spartanullnull.otil.domain.like.entity.Like;
import com.spartanullnull.otil.domain.like.repository.LikeRepository;
import com.spartanullnull.otil.domain.user.entity.User;
import com.spartanullnull.otil.domain.user.repository.UserRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CommentLikeService {

    private final LikeRepository likeRepository;
    private final UserRepository userRepository;
    private final CommentRepository commentRepository;

    /**
     * 사용자가 댓글을 좋아요를 누르는 메서드
     *
     * @param userId        좋아요를 누르는 사용자
     * @param commentId     좋아요를 누를 댓글 ID
     */
    @Transactional
    public void createLikeForComment(Long userId, Long commentId) {

        User user = userRepository.findById(userId)
            .orElseThrow(() -> new EntityNotFoundException("사용자를 찾을 수 없습니다."));

        Comment comment = commentRepository.findById(commentId)
            .orElseThrow(() -> new EntityNotFoundException("댓글을 찾을 수 없습니다."));

        if (likeRepository.existsByUserAndComment(user, comment)) {
            // 좋아요가 이미 존재하면 처리하지 않음
            return;
        }

        likeRepository.save(Like.builder().user(user).comment(comment).build());
    }

    /**
     * 사용자가 댓글에 좋아요를 취소하는 메서드
     *
     * @param userId      좋아요 취소하는 사용자
     * @param commentId   좋아요를 취소할 댓글 ID
     */
    @Transactional
    public void deleteLikeForComment(Long userId, Long commentId) {

        User user = userRepository.findById(userId)
            .orElseThrow(() -> new EntityNotFoundException("사용자를 찾을 수 없습니다."));

        Comment comment = commentRepository.findById(commentId)
            .orElseThrow(() -> new EntityNotFoundException("댓글을 찾을 수 없습니다."));

        if (likeRepository.existsByUserAndComment(user, comment)) {
            likeRepository.deleteByUserAndComment(user, comment);
        }
    }

    /**
     *
     * @param commentId   조회할 댓글 ID
     * @return            댓글의 좋아요 개수
     */
    public Long getLikeCountForComment(Long commentId) {
        return likeRepository.countByCommentId(commentId);
    }

}