package com.raphaelframos.refii.chat;

import com.raphaelframos.refii.common.entity.ChatFundEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ChatRepository extends JpaRepository<ChatFundEntity, Long> {

    @Query("FROM CHAT_FUND c JOIN FETCH c.profileEntity p WHERE c.position = ?1 AND p.id = ?2")
    Optional<ChatFundEntity> findByPositionAndUserId(int position, Long userId);

    @Query("FROM CHAT_FUND c JOIN FETCH c.profileEntity p WHERE p.id = ?1 ORDER BY c.position")
    List<ChatFundEntity> findByUserId(Long userId);

    @Query("DELETE FROM CHAT_FUND")
    void delete(Long userId);
}