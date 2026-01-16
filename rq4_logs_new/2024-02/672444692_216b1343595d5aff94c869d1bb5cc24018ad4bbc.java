package kr.ac.kumoh.illdang100.tovalley.kafka;

import java.io.Serializable;
import javax.validation.constraints.NotNull;
import kr.ac.kumoh.illdang100.tovalley.domain.chat.ChatMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
// Kafka를 통해 전송되는 메시지의 데이터 구조
/*
메시지는 Producer에서 생성되어 Kafka Broker로 전송되고, 이후 Consumer가 해당 메시지를 받아 처리하게 된다.
이때 메시지의 데이터 형태를 일관성 있게 유지하기 위해 Message 클래스와 같은 도메인 모델을 사용한다.
 */
public class Message implements Serializable {

    private String id;

    @NotNull
    private Long chatRoomId;

    @NotNull
    private Long senderId;

    @NotNull
    private String content;

    private String createdAt;
    private Integer readCount;

//    @NotNull
//    private String contentType;

//    private String senderName;
//    private Integer senderNo;
//    private String senderEmail;

    public void setId(String id) {
        this.id = id;
    }

    //    public void setSendTimeAndSender(LocalDateTime sendTime, Integer senderNo, String senderName, Integer readCount) {
//        this.senderName = senderName;
//        this.sendTime = sendTime.atZone(ZoneId.of("Asia/Seoul")).toInstant().toEpochMilli();
//        this.senderNo = senderNo;
//        this.readCount = readCount;
//    }

    public ChatMessage convertEntity() {
        return ChatMessage.builder()
                .id(id)
                .chatRoomId(chatRoomId)
                .senderId(senderId)
                .content(content)
                .createdAt(createdAt)
                .readCount(readCount)
                .build();
    }
}
