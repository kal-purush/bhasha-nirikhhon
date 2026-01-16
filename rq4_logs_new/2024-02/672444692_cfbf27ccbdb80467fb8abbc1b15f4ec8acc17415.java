package kr.ac.kumoh.illdang100.tovalley.domain.chat.redis;

import java.time.LocalDateTime;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.index.Indexed;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@RedisHash(value = "chatRoomParticipant")
public class ChatRoomParticipant {

    @Id
    private String id;

    @Indexed
    private Long memberId;

    @Indexed
    private Long chatRoomId;

    private LocalDateTime joinedAt;

    public ChatRoomParticipant(Long memberId, Long chatRoomId) {
        this.memberId = memberId;
        this.chatRoomId = chatRoomId;
        this.joinedAt = LocalDateTime.now();
    }
}