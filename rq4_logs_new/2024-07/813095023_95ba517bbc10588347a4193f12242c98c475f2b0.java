package com.spring.mood.projectmvc.repository;

import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.spring.mood.projectmvc.entity.ChatRoom;
import com.spring.mood.projectmvc.entity.QChatRoom;
import com.spring.mood.projectmvc.entity.QTopic;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
@RequiredArgsConstructor
public class ChatRoomRepositoryCustomImpl implements ChatRoomRepositoryCustom {
    private final JPAQueryFactory jpaQueryFactory;
    @Override
    public List<Tuple> findTopicRooms(Integer topicId) {
        QChatRoom chatRoom = QChatRoom.chatRoom;
        QTopic topic = QTopic.topic;

        return jpaQueryFactory.select(chatRoom, topic)
                .from(chatRoom)
                .join(chatRoom.topic, topic)
                .where(topic.topicId.eq(topicId))
                .fetch();
    }

    @Override
    public ChatRoom findChatRoomByTopicIdAndRoomId(Integer topicId, int roomId) {
        QChatRoom chatRoom = QChatRoom.chatRoom;
        QTopic topic = QTopic.topic;

        return jpaQueryFactory.selectFrom(chatRoom)
                .join(chatRoom.topic, topic)
                .where(topic.topicId.eq(topicId).and(chatRoom.roomId.eq(roomId)))
                .fetchOne();
    }
}