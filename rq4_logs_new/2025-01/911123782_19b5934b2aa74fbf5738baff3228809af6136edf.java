package com.roomfit.be.chat.infrastructure.support;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.roomfit.be.chat.domain.Message;
import com.roomfit.be.chat.domain.QMessage;
import com.roomfit.be.global.response.PaginationResponse;
import com.roomfit.be.user.domain.QUser;
import com.roomfit.be.user.domain.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Slf4j
@RequiredArgsConstructor
public class ChatRoomPaginationRepositoryImpl implements ChatRoomPaginationRepository {
    private final JPAQueryFactory queryFactory;
    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public List<Message> findMessagesByChatRoomIdWithOffset(Long chatRoomId, int offset, int pageSize) {
        QMessage message = QMessage.message;
        QUser user = QUser.user;

        List<Tuple> tuples = queryFactory
                .select(message, user)
                .from(message)
                .leftJoin(user).on(user.id.eq(message.senderId))
                .where(message.chatRoomId.eq(chatRoomId))
                .orderBy(message.id.asc())
                .offset(offset)
                .limit(pageSize)
                .fetch();

        return tuples.stream()
                .map(tuple -> {
                    Message msg = tuple.get(message);
                    User sender = tuple.get(user);
                    assert msg != null;
                    msg.setSender(sender);
                    return msg;
                })
                .toList();
    }

    @Override
    public PaginationResponse<Message> findMessagesByChatRoomIdPaginated(Long chatRoomId, Long lastMessageId, int pageSize) {
        List<Message> messages = fetchMessages(chatRoomId, lastMessageId, pageSize + 1);  // 1개 더 들고오기
        long totalCount = getTotalMessageCount(chatRoomId);
        List<Message> actualMessages = adjustMessageListSize(messages, pageSize);
        boolean hasNext = messages.size() > pageSize;
        return new PaginationResponse<>(actualMessages, totalCount, hasNext);
    }

    private List<Message> fetchMessages(Long chatRoomId, Long lastMessageId, int pageSize) {
        return queryFactory
                .selectFrom(QMessage.message)
                .leftJoin(QUser.user).on(QUser.user.id.eq(QMessage.message.senderId))
                .where(QMessage.message.chatRoomId.eq(chatRoomId)
                        .and(getLastMessageCondition(lastMessageId)))
                .orderBy(QMessage.message.id.desc())
                .limit(pageSize)
                .fetch()
                .stream()
                .toList();
    }
    private BooleanExpression getLastMessageCondition(Long lastMessageId) {
        if (lastMessageId == null) {
            return null;
        }
        return QMessage.message.id.lt(lastMessageId);
    }
    private List<Message> adjustMessageListSize(List<Message> messages, int pageSize) {
        if (messages.size() > pageSize) {
            return messages.subList(0, pageSize);
        }
        return messages;
    }

    private long getTotalMessageCount(Long chatRoomId) {
        String key = "chatroom:" + chatRoomId;
        String countStr = redisTemplate.opsForValue().get(key);
        return Long.parseLong(countStr);
    }
}
