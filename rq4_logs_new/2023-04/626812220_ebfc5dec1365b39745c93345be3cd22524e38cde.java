package com.billi.chatting;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.billi.frontcontroller.CommonControllerInterface;
import com.billi.model.BoardsService;
import com.billi.model.ChattingService;
import com.billi.vo.ChatRoomVO;
import com.billi.vo.ChatVO;

public class ChattingController implements CommonControllerInterface {

	@Override
	public String excute(Map<String, Object> data) throws Exception {
		HttpServletRequest request = (HttpServletRequest) data.get("request");
		String method = (String)data.get("method");
		//String page = "/chatting.jsp";
		String page = "/chat/chatting.jsp";
		
		if(method.equals("GET")) {
			String seller=request.getParameter("seller");
			String buyer=request.getParameter("buyer");
			int board=Integer.parseInt(request.getParameter("board"));
			
			//채팅룸
			ChattingService service = new ChattingService();
			BoardsService bService = new BoardsService();
			ChatRoomVO chatRoom = new ChatRoomVO();
			
			chatRoom.setBuyer(buyer);
			chatRoom.setSeller(seller);
			chatRoom.setBoard_id(board);
			chatRoom.setBoard_title(bService.TitleById(board));
			
			//채팅방 있는지 확인 있으면 내용 불러오기
			if (service.countByChatId(chatRoom.getBoard_id(), chatRoom.getBuyer()) > 0) {
	            //get ChatRoomInfo
				chatRoom = service.findRoom(chatRoom.getBoard_id(), chatRoom.getBuyer());
	            //load existing chat history
	            List<ChatVO> chatHistory = service.findChatById(chatRoom.getRoom_id());
	            System.out.println("history"+chatHistory);
	            request.setAttribute("chatHistory", chatHistory);
	            
	        } else {
	            //chatRoom 생성            
	        	service.addChatRoom(chatRoom); 
	        	chatRoom = service.findRoom(chatRoom.getBoard_id(), chatRoom.getBuyer());
	        	request.setAttribute("chatHistory", null);
	        }
            //채팅방 정보 전달
			request.setAttribute("chatRoom", chatRoom);
		}else { 
		}
		
		return page;
	}

}