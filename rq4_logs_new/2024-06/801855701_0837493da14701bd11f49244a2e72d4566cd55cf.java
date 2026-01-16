package com.example.chattingroom.config;

import com.example.chattingroom.exception.UnvalidatedTokenException;
import com.example.chattingroom.util.JwtUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StompHandler implements ChannelInterceptor {

    private final JwtUtil jwtUtil;
    private final static String HEADER_AUTHORIZATION = "Authorization";

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);

        if (accessor.getCommand() == StompCommand.CONNECT){
            String token = accessor.getFirstNativeHeader(HEADER_AUTHORIZATION);
            log.info("token = {}", token);
            if (token == null || !jwtUtil.validToken(token)) {
                throw new UnvalidatedTokenException("unvalidated token");
            }
            log.info("user connected");
        }
        // todo
        return message;
    }
}