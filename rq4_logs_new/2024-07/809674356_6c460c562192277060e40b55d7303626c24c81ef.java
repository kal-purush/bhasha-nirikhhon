package socket;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Controller;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import service.IF_ChatContService;
import vo.ChatContVO;

@Controller
public class SocketHandler extends TextWebSocketHandler {
	@Inject
	IF_ChatContService ccServe;
	
	private List<WebSocketSession> userList = new ArrayList<>();

	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		System.out.println("성공");
		System.out.println(session.getAttributes());
		System.out.println(session.getAttributes().get("userid"));
		userList.add(session);
		super.afterConnectionEstablished(session);
		System.out.println(userList.size());
	}

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		System.out.println("보내기 성공");
		System.out.println(message.getPayload());
		super.handleTextMessage(session, message);
		ObjectMapper mapper = new ObjectMapper();
		try {
			ChatContVO ccVO = mapper.readValue(message.getPayload(), ChatContVO.class);
			System.out.println(ccVO.getChatNum());
			System.out.println(ccVO.getChatAttach());
			System.out.println(ccVO.getNickName());
			System.out.println(ccVO.getCont());
			ccServe.insert(ccVO);
		} catch (JsonProcessingException e) {
		    e.printStackTrace(); // 예외 메시지 출력
		} catch (Exception e) {
		    e.printStackTrace();
		}
		for(WebSocketSession s : userList) {
			if(s.getAttributes().get("nowChat").equals(session.getAttributes().get("nowChat"))) {
				s.sendMessage(new TextMessage(message.getPayload()));
			}
		}
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		// TODO Auto-generated method stub
		super.afterConnectionClosed(session, status);
		System.out.println(userList.size());
	}

}