package com.team2.lb.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import com.team2.lb.handler.ChatHandler;

@EnableWebSocketMessageBroker
@Configuration
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer{
	
	// private final ChatHandler chatHandler;
	
    //endpoint를 /stomp로 하고, allowedOrigins를 "*"로 하면 페이지에서
    //Get /info 404 Error가 발생한다. 그래서 아래와 같이 2개의 계층으로 분리하고
    //origins를 개발 도메인으로 변경하니 잘 동작하였다.
    //이유는 왜 그런지 아직 찾지 못함
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/stomp/chat")
                .setAllowedOrigins("http://localhost:8000")
                .withSockJS();
    }
/*
	@Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(chatHandler, "/ws/chat")
                .setAllowedOriginPatterns("http://*:8000", "http://*.*.*.*:8000")
                .withSockJS()
                .setClientLibraryUrl("http://localhost:8080/myapp/js/sock-client.js");
                //.setClientLibarayUrl은 그냥 sockjs CDN 주소를 입력해도 무관하다.
                //https://cdnjs.cloudflare.com/ajax/libs/sockjs-client/1.1.2/sockjs.js
                */
                /*
                Spring Boot에서 CORS 설정 시, .allowCredentials(true)와
                .allowedOrigins("*")는 동시 설정을 못하도록 업데이트 되었다고 한다.
                모든 주소를 허용하는 대신 특정 패턴만 허용하는 것으로 적용해야한다고 변동됨.
                
                .allowedOrigins("*") 대신 .allowedOriginPatterns("*")를 사용하면 에러는 해결이
                된다고 한다.
                
                나는 이처럼 하지 않고, http://localhost:8080 또는, IP 주소로 접속하기 때문에
                위에 설정처럼 하였다.

                */
	/*
    }
*/
	
    /*어플리케이션 내부에서 사용할 path를 지정할 수 있음*/
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes("/pub");
        registry.enableSimpleBroker("/sub");
    }


}