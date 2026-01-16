    package com.roomfit.be.chat.infrastructure.aop;

    import com.roomfit.be.chat.application.dto.MessageDTO;
    import com.roomfit.be.chat.infrastructure.MessageRepository;
    import jakarta.annotation.PostConstruct;
    import jakarta.persistence.Tuple;
    import lombok.RequiredArgsConstructor;
    import lombok.extern.slf4j.Slf4j;
    import org.aspectj.lang.ProceedingJoinPoint;
    import org.aspectj.lang.annotation.Around;
    import org.aspectj.lang.annotation.Aspect;
    import org.springframework.data.redis.core.RedisTemplate;
    import org.springframework.stereotype.Component;

    import java.util.List;
    import java.util.Map;
    import java.util.stream.Collectors;

    @Aspect
    @Component
    @RequiredArgsConstructor
    @Slf4j
    public class MessageCountAspect {
        private static final String PREFIX = "chatroom:";
        private final RedisTemplate<String, String> redisTemplate;
        private final MessageRepository messageRepository;
        @PostConstruct
        public void initializeMessageCount() {
            List<Tuple> results = messageRepository.countMessagesByRoomIds();

            Map<Long, Long> messageCounts = results.stream()
                    .collect(Collectors.toMap(
                            tuple -> tuple.get("roomId", Long.class),  // roomId 추출
                            tuple -> tuple.get("messageCount", Long.class)  // messageCount 추출
                    ));

            messageCounts.forEach((roomId, messageCount) -> {
                String key = PREFIX + roomId;
                redisTemplate.opsForValue().set(key, String.valueOf(messageCount));
            });
        }

        @Around("@annotation(com.roomfit.be.chat.application.annotation.MessageCountProcessor)")
        public Object processMessageCount(ProceedingJoinPoint joinPoint) throws Throwable {
            Object result = joinPoint.proceed();

            Object[] args = joinPoint.getArgs();
            Long roomId = ((MessageDTO.Send)args[0]).getRoomId();

            String key = PREFIX + roomId;
            redisTemplate.opsForValue().increment(key, 1);

            return result;
        }
    }