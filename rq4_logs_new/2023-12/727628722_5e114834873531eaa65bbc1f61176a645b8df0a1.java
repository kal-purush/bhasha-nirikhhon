package com.spartanullnull.otil.domain.recommend.service;

import com.spartanullnull.otil.domain.post.entity.Post;
import com.spartanullnull.otil.domain.post.repository.PostRepository;
import com.spartanullnull.otil.domain.recommend.entity.Recommend;
import com.spartanullnull.otil.domain.recommend.repository.RecommendRepository;
import com.spartanullnull.otil.domain.user.entity.User;
import com.spartanullnull.otil.domain.user.repository.UserRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class RecommendService {

    private final RecommendRepository recommendRepository;
    private final UserRepository userRepository;
    private final PostRepository postRepository;

    /**
     * 사용자가 게시물을 추천하는 메서드
     *
     * @param userId    추천을 누르는 사용자
     * @param postId    추천할 게시물 ID
     */
    @Transactional
    public void createRecommend(Long userId, Long postId) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new EntityNotFoundException("사용자를 찾을 수 없습니다."));

        Post post = postRepository.findById(postId)
            .orElseThrow(() -> new EntityNotFoundException("게시물을 찾을 수 없습니다."));

        if (recommendRepository.existsByUserAndPost(user, post)) {
            // 추천이 이미 존재하면 처리하지 않음
            return;
        }

        recommendRepository.save(Recommend.builder().user(user).post(post).build());
    }

    /**
     * 사용자가 게시물 추천을 취소하는 메서드
     *
     * @param userId    추천 취소하는 사용자
     * @param postId    추천을 취소할 게시물 ID
     */
    @Transactional
    public void deleteRecommend(Long userId, Long postId) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new EntityNotFoundException("사용자를 찾을 수 없습니다."));

        Post post = postRepository.findById(postId)
            .orElseThrow(() -> new EntityNotFoundException("게시물을 찾을 수 없습니다."));

        if (recommendRepository.existsByUserAndPost(user, post)) {
            recommendRepository.deleteByUserAndPost(user, post);
        }
    }

    /**
     *
     * @param postId    조회할 게시물 ID
     * @return          게시물의 추천 개수
     */

    public Long getRecommendCount(Long postId) {
        return recommendRepository.countByPostId(postId);
    }
}