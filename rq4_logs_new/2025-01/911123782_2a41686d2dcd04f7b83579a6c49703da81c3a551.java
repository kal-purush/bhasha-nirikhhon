package com.roomfit.be.participation.infrastructure;

import com.roomfit.be.participation.domain.Participation;
import com.roomfit.be.user.domain.User;
import io.lettuce.core.dynamic.annotation.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface ParticipationRepository extends JpaRepository<Participation, Long> {
    @Query("SELECT p.user FROM participation p WHERE p.chatRoom.id = :roomId")
    List<User> findUsersByChatRoomId(@Param("roomId") Long roomId);
}