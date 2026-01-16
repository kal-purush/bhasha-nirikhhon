package kr.ac.kumoh.illdang100.tovalley.domain.chat;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import kr.ac.kumoh.illdang100.tovalley.domain.BaseTimeEntity;
import kr.ac.kumoh.illdang100.tovalley.domain.member.Member;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class ChatNotification extends BaseTimeEntity {

    // TODO: 알림 확장성 고려하기(채팅, 댓글, 게시글 등등)

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "chat_notification_id", nullable = false)
    private String id;

    // 상대방의 정보가 변경되면 반영해서 보여줄 것이므로 연관 관계를 맺도록 한다.
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sender_id", nullable = false)
    private Member sender;

    @Column(name = "recipient_id", nullable = false)
    private Long recipientId;

    @Column(name = "chat_room_id", nullable = false)
    private Long chatRoomId;

    // 프라이버시로 채팅 메시지는 사용자에게 알려주지 않기!!
    // 채팅방 목록 조회 페이지에서 업데이트하려면 내용도 포함해야함
    @Column(name = "content", nullable = false)
    private String content;

//    private String chatMessageId;

    @Column(name = "has_read", nullable = false)
    private Boolean hasRead;

    public ChatNotification(Member sender, Long recipientId, Long chatRoomId, String content) {
        this.sender = sender;
        this.recipientId = recipientId;
        this.chatRoomId = chatRoomId;
        this.content = content;
        this.hasRead = false;
    }
}