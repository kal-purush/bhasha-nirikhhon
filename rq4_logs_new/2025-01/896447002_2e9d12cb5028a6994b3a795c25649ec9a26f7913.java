package com.example.musing.like_music.repository;

import com.example.musing.like_music.entity.Like_Music;
import com.example.musing.user.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface Like_MusicRepository extends JpaRepository<Like_Music, Long> {
    @Query("SELECT lm FROM Like_Music lm WHERE lm.user.id = :id ORDER BY lm.id DESC")
    List<Like_Music> findTop10ByUserOrderByIdDesc(@Param("id") String userId);
}