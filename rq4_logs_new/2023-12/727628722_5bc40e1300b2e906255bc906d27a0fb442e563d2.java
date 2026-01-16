package com.spartanullnull.otil.domain.like.service;

import com.spartanullnull.otil.domain.like.entity.Like;
import com.spartanullnull.otil.domain.like.repository.LikeRepository;
import com.spartanullnull.otil.domain.post.entity.Post;
import com.spartanullnull.otil.domain.post.repository.PostRepository;
import com.spartanullnull.otil.domain.user.entity.User;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LikeService {

    private final LikeRepository likeRepository;
    private final PostRepository postRepository;

    public Like createLike(User user, Post post) {
        // 중복 좋아요 확인 및 예외 처리 로직 추가
        return likeRepository.save(Like.builder().user(user).post(post).build());
    }

    public boolean isLikedByUser(User user, Post post) {
        return likeRepository.existsByUserAndPost(user, post);
    }

    /**
     * 사용자가 TIL에 좋아요를 누르는 메서드
     *
     * @param user      좋아요를 누르는 사용자
     * @param postId    좋아요를 누를 게시물 ID
     */
    public void createLikeForPost(User user, Long postId) {
        Post post = postRepository.findById(postId)
            .orElseThrow(() -> new EntityNotFoundException("TIL을 찾을 수 없습니다."));

        if (likeRepository.existsByUserAndPost(user, post)) {
            likeRepository.save(Like.builder().user(user).post(post).build());
        } else {
            deleteLikeForPost(user, postId);
        }
    }

    /**
     * 사용자가 TIL에 좋아요를 취소하는 메서드
     *
     * @param user      좋아요 취소하는 사용자
     * @param postId    좋아요를 취소할 게시물 ID
     */
    public void deleteLikeForPost(User user, Long postId) {
        Post post = postRepository.findById(postId)
            .orElseThrow(() -> new EntityNotFoundException("TIL을 찾을 수 없습니다."));

        if (likeRepository.existsByUserAndPost(user, post)) {
            likeRepository.deleteByUserAndPost(user, post);
        }
    }
}