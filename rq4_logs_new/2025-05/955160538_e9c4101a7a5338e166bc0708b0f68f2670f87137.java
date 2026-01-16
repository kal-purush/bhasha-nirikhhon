package com.iot7.repository;

import com.iot7.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface PushTokenRepository extends JpaRepository<User, String> {

    // ✅ userId로 해당 유저의 pushToken 조회
    @Query("SELECT u.pushToken FROM User u WHERE u.userId = :userId")
    String findPushTokenByUserId(@Param("userId") Long userId);

    // ✅ 동일한 푸시 토큰을 가진 유저 목록
    List<User> findByPushToken(String pushToken);

    // ✅ userId로 유저 조회 (save 전에 필요)
    Optional<User> findById(String userId);
}