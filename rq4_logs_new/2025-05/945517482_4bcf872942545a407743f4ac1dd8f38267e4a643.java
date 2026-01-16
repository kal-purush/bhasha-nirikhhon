package com.gitprism.GitPRism.portfolios.listener;

import com.gitprism.GitPRism.portfolios.dto.websocket.ActiveUserMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class PortfolioWebSocketEventListener {

  private final SimpMessagingTemplate messagingTemplate;

  // 포트폴리오 ID별 editor 세션 저장
  private final Map<Long, Set<String>> activeUsersMap = new ConcurrentHashMap<>();

  @EventListener
  public void handleConnect(SessionConnectEvent event) {
    StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());

    Long portfolioId = parseLong(accessor.getFirstNativeHeader("portfolioId"));
    String editorName = accessor.getFirstNativeHeader("editorName");

    if (portfolioId != null && editorName != null) {
      activeUsersMap
          .computeIfAbsent(portfolioId, k -> ConcurrentHashMap.newKeySet())
          .add(editorName);
      broadcastActiveUsers(portfolioId);
    }
  }

  @EventListener
  public void handleDisconnect(SessionDisconnectEvent event) {
    StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());

    Long portfolioId = parseLong(accessor.getFirstNativeHeader("portfolioId"));
    String editorName = accessor.getFirstNativeHeader("editorName");

    if (portfolioId != null && editorName != null) {
      Set<String> users = activeUsersMap.getOrDefault(portfolioId, new HashSet<>());
      users.remove(editorName);
      if (users.isEmpty()) {
        activeUsersMap.remove(portfolioId);
      }
      broadcastActiveUsers(portfolioId);
    }
  }

  private void broadcastActiveUsers(Long portfolioId) {
    List<String> users = new ArrayList<>(activeUsersMap.getOrDefault(portfolioId, Set.of()));
    messagingTemplate.convertAndSend(
        "/topic/active." + portfolioId,
        new ActiveUserMessage(portfolioId, users)
    );
    log.info("🔄 접속 사용자 목록 전송: {}", users);
  }

  private Long parseLong(String value) {
    try {
      return Long.parseLong(value);
    } catch (Exception e) {
      return null;
    }
  }
}