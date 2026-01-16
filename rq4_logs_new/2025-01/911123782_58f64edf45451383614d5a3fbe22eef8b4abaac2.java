package com.roomfit.be.chat.presentation;

import com.roomfit.be.auth.application.dto.UserDetails;
import com.roomfit.be.chat.application.ChatRoomService;
import com.roomfit.be.chat.application.dto.ChatRoomDTO;
import com.roomfit.be.global.annontation.AuthCheck;
import com.roomfit.be.global.response.CommonResponse;
import com.roomfit.be.global.response.ResponseFactory;
import io.swagger.v3.oas.annotations.Parameter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/chats")
@RequiredArgsConstructor
public class ChatController {
    private final ChatRoomService chatService;

    /**
     * 채팅방 생성
     */
    @AuthCheck()
    @PostMapping("")
    public ResponseEntity<CommonResponse<ChatRoomDTO.Response>> createChatRoom(
            UserDetails userDetails,
            @RequestBody ChatRoomDTO.Create request) {

        ChatRoomDTO.Response response = chatService.createRoom(userDetails.getId(), request);
        CommonResponse<ChatRoomDTO.Response> commonResponse = ResponseFactory.success(response, "채팅방 생성 성공");
        return ResponseEntity
                .status(HttpStatus.CREATED)  // 201 상태 코드
                .body(commonResponse);  // 응답 본문
    }

    /**
     * 채팅방 참여
     */
    @AuthCheck()
    @PostMapping("/{chat_id}/join")
    public ResponseEntity<CommonResponse<ChatRoomDTO.Response>> enterChatRoom(
            UserDetails userDetails,
            @Parameter(description = "채팅방 ID") @PathVariable("chat_id") Long chatId) {

        ChatRoomDTO.Response response = chatService.enterRoom(userDetails.getId(), chatId);
        CommonResponse<ChatRoomDTO.Response> commonResponse = ResponseFactory.success(response, "채팅방 참여 성공");
        return ResponseEntity
                .status(HttpStatus.OK)  // 200 상태 코드
                .body(commonResponse);  // 응답 본문
    }

    /**
     * 채팅방 퇴장
     */
    @AuthCheck()
    @PostMapping("/{chat_id}/leave")
    public ResponseEntity<CommonResponse<String>> leaveChatRoom(
            UserDetails userDetails,
            @Parameter(description = "채팅방 ID") @PathVariable("chat_id") Long chatId) {

        // 채팅방 퇴장 로직 (실제로는 로직 추가 필요)
        CommonResponse<String> commonResponse = ResponseFactory.success("채팅방 퇴장 성공");
        return ResponseEntity
                .status(HttpStatus.OK)  // 200 상태 코드
                .body(commonResponse);  // 응답 본문
    }

    /**
     * 채팅방 메시지 조회
     */
    @GetMapping("/{chat_id}/messages")
    public ResponseEntity<CommonResponse<String>> readMessagesByChatId(
            @PathVariable("chat_id") Long chatId) {
        // 메시지 조회 로직 (실제로는 로직 추가 필요)
        CommonResponse<String> commonResponse = ResponseFactory.success("메시지 조회 성공");
        return ResponseEntity
                .status(HttpStatus.OK)  // 200 상태 코드
                .body(commonResponse);  // 응답 본문
    }
}