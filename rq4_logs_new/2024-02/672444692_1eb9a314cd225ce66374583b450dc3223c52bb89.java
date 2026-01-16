package kr.ac.kumoh.illdang100.tovalley.service.chat;

import kr.ac.kumoh.illdang100.tovalley.domain.chat.kafka.Message;
import kr.ac.kumoh.illdang100.tovalley.domain.chat.kafka.Notification;
import kr.ac.kumoh.illdang100.tovalley.util.KafkaVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaReceiver {

    private final SimpMessageSendingOperations template;

    @KafkaListener(topics = KafkaVO.KAFKA_CHAT_TOPIC, containerFactory = "kafkaChatContainerFactory")
    public void receiveChatMessage(Message message) {
        log.debug("전송 위치 = /sub/chat/room/"+ message.getChatRoomId());
        log.debug("채팅 방으로 메시지 전송 = {}", message);

        template.convertAndSend("/sub/chat/room/" + message.getChatRoomId(), message);
    }

    @KafkaListener(topics = KafkaVO.KAFKA_NOTIFICATION_TOPIC, containerFactory = "kafkaNotificationContainerFactory")
    public void receiveNotificationMessage(Notification notification) {
        log.debug("전송 위치 = /sub/notification/"+ notification.getRecipientId());
        log.debug("알림 메시지 전송 = {}", notification);

        template.convertAndSend("/sub/notification/" + notification.getRecipientId(), notification);
    }
}