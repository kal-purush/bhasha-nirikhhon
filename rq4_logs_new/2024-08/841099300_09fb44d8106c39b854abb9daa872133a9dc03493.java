package com.example.hackathon.user.repository;

import com.example.hackathon.user.domain.UserSearch;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserSearchRepository extends JpaRepository<UserSearch, Long> {
    @Query("SELECT us FROM UserSearch us WHERE us.user.id=:userId ")
    List<UserSearch> findByRecentKeyword(@Param("userId")Long userId, Pageable pageable);
}