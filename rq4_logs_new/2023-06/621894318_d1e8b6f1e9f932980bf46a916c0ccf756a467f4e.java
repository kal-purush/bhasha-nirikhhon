package com.gxdxx.instagram.service.follow;

import com.gxdxx.instagram.dto.request.FollowCreateRequest;
import com.gxdxx.instagram.dto.response.SuccessResponse;
import com.gxdxx.instagram.entity.Follow;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.FollowAlreadyExistsException;
import com.gxdxx.instagram.exception.InvalidRequestException;
import com.gxdxx.instagram.exception.UserNotFoundException;
import com.gxdxx.instagram.repository.FollowRepository;
import com.gxdxx.instagram.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@RequiredArgsConstructor
@Service
public class FollowCreateService {

    private final FollowRepository followRepository;
    private final UserRepository userRepository;

    public SuccessResponse createFollow(FollowCreateRequest request, String followerNickname) {

        User follower = userRepository.findByNickname(followerNickname)
                .orElseThrow(UserNotFoundException::new);
        User following = userRepository.findById(request.userId())
                .orElseThrow(UserNotFoundException::new);

        if (follower.equals(following)) {
            throw new InvalidRequestException();
        }

        if (followRepository.existsByFollowerAndFollowing(follower, following)) {
            throw new FollowAlreadyExistsException();
        }

        Follow follow = followRepository.findByFollowerAndFollowingAndDeleted(follower, following, true)
                .orElse(Follow.createFollow(follower, following));
        follow.changeDeleted(false);

        followRepository.save(follow);
        return SuccessResponse.of("200 SUCCESS");
    }

}