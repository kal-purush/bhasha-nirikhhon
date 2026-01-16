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
		boolean flag = true;
		for(WebSocketSession s : userList) {
			if(s.getAttributes().get("nickName").equals(session.getAttributes().get("nickName"))
					&& s.getAttributes().get("nowChat").equals(session.getAttributes().get("nowChat"))) {
				s = session;
				flag = false;
			}
		}
		if(flag) {
			userList.add(session);
		}
		super.afterConnectionEstablished(session);
		System.out.println(userList.size());
	}

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		System.out.println("보내기 성공");
		for(WebSocketSession s : userList) {
			System.out.println(s.getAttributes().get("nowChat"));
		}
		System.out.println(message.getPayload());
		super.handleTextMessage(session, message);
		ObjectMapper mapper = new ObjectMapper();
		List<ChatContVO> cList = new ArrayList<>();
		ChatContVO ccVO = mapper.readValue(message.getPayload(), ChatContVO.class);
		if(ccVO.getCont() != null && !ccVO.getCont().trim().equals("")) {
			cList.add(ccServe.insert(ccVO));
		}
		if(ccVO.getChatAttach().equals("YES")) {
			List<ChatContVO> fileList = ccServe.selectAttachList(ccVO);
			for(ChatContVO c : fileList) {
				cList.add(c);
			}
		}
		System.out.println("여기까지 성공");
		for(WebSocketSession s : userList) {
			if(s.getAttributes().get("nowChat").equals(session.getAttributes().get("nowChat"))) {
				System.out.println("성공");
				try {
					for(ChatContVO cc : cList) {
						s.sendMessage(new TextMessage(mapper.writeValueAsString(cc)));
					}
				} catch(Exception e) {
					System.out.println("socket 오류 : "+e);
				}
			}
		}
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		// TODO Auto-generated method stub
		super.afterConnectionClosed(session, status);
		userList.remove(session);
	}

}