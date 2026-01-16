package com.metamong.mt.global.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.metamong.mt.domain.member.controller.MemberController;

@Component
public class MyWebSocketHandler extends TextWebSocketHandler {

	@Autowired
    private MemberController boardController;



    // WebSocket 연결 시 세션을 추가
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        boardController.addSession(session); // WebSocket 세션을 BoardController에 추가
    }

    // 클라이언트로부터 받은 메시지를 처리하고, 클라이언트에게 응답을 보냄
    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        System.out.println("Received message: " + message.getPayload());
        String responseMessage = "서버에서 받은 답변: " + message.getPayload();
        session.sendMessage(new TextMessage(responseMessage)); // 클라이언트로 메시지 전송
    }
}