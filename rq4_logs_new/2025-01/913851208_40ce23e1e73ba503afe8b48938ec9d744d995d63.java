package com.project_merge.jigu_travel.api.ai_guide.controller;

import com.project_merge.jigu_travel.api.ai_guide.dto.AiGuideResponse;
import com.project_merge.jigu_travel.api.ai_guide.dto.UserInputRequestDto;
import com.project_merge.jigu_travel.api.ai_guide.entity.ConversationHistory;
import com.project_merge.jigu_travel.api.ai_guide.fast.FastApiClient;
import com.project_merge.jigu_travel.api.ai_guide.service.ConversationHistoryService;
import com.project_merge.jigu_travel.api.user.service.UserService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

@RestController  // REST API 요청을 처리하는 컨트롤러
@RequiredArgsConstructor
@RequestMapping("/api/ai-guide")
public class AiGuideRestController {
    private final FastApiClient fastApiClient; //fast api
    private final UserService userService; //user_id 불러오기
    private final ConversationHistoryService conversationHistoryService;

    // 음성 녹음 파일 처리
    @PostMapping("/upload-audio")
    public AiGuideResponse uploadAudio(@RequestParam("audio") MultipartFile audio, HttpSession session) {
        try {
            // 세션에서 대화 기록 가져오기 (없으면 빈 대화 기록 생성)
            UserInputRequestDto.ConversationHistory conversationHistory =
                    (UserInputRequestDto.ConversationHistory) session.getAttribute("conversation_history");

            if (conversationHistory == null) {
                conversationHistory = new UserInputRequestDto.ConversationHistory();
                conversationHistory.setHistory(new ArrayList<>());
            }

            // UserInput 생성 (이전 대화 기록을 포함)
            UserInputRequestDto userInput = new UserInputRequestDto();
            userInput.setUser_question("불광사가 뭐야?");
            userInput.setUser_category(Arrays.asList("맛집", "힐링"));
            userInput.setLatitude(37.508373);
            userInput.setLongitude(127.103565);
            userInput.setConversation_history(conversationHistory);

            // FastAPI에 요청 보내기
            AiGuideResponse response = fastApiClient.getAiGuideTest(userInput);
            // 응답 로깅 추가
            System.out.println("FastAPI 응답: " + response.getAnswer());

            // 새로운 대화 항목 추가
            UserInputRequestDto.ConversationHistory.ConversationHistoryItem newItem =
                    new UserInputRequestDto.ConversationHistory.ConversationHistoryItem();
            newItem.setUser_question(userInput.getUser_question());
            newItem.setAssistant_response(response.getAnswer());

            // 대화 기록을 1개만 저장
            conversationHistory.getHistory().clear(); // 기존 기록 삭제
            conversationHistory.getHistory().add(newItem);

            // 대화 기록을 세션에 저장
            session.setAttribute("conversation_history", conversationHistory);

            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return new AiGuideResponse("API 요청 실패", null);
        }
    }

    // 텍스트 질문 처리
    @PostMapping("/upload-text")
    public AiGuideResponse uploadText(@RequestBody Map<String, String> requestBody, HttpSession session) {

        try {

            String userQuestion = requestBody.get("user_question"); // JSON에서 'user_question' 값을 가져옴
            // 세션에서 대화 기록 가져오기 (없으면 빈 대화 기록 생성)
            UserInputRequestDto.ConversationHistory conversationHistory =
                    (UserInputRequestDto.ConversationHistory) session.getAttribute("conversation_history");

            if (conversationHistory == null) {
                conversationHistory = new UserInputRequestDto.ConversationHistory();
                conversationHistory.setHistory(new ArrayList<>());
            }

            // UserInput 생성 (이전 대화 기록을 포함)
            UserInputRequestDto userInput = new UserInputRequestDto();
            userInput.setUser_question(userQuestion);
            userInput.setUser_category(Arrays.asList("맛집", "힐링")); //수정예정
            userInput.setLatitude(37.508373);//수정예정
            userInput.setLongitude(127.103565);//수정예정
            userInput.setConversation_history(conversationHistory);

            // FastAPI에 요청 보내기
            AiGuideResponse response = fastApiClient.getAiGuideTest(userInput);

            // 응답 로깅 추가
            System.out.println("FastAPI 응답: " + response.getAnswer());

            // 새로운 대화 항목 추가
            UserInputRequestDto.ConversationHistory.ConversationHistoryItem newItem =
                    new UserInputRequestDto.ConversationHistory.ConversationHistoryItem();
            newItem.setUser_question(userInput.getUser_question());
            newItem.setAssistant_response(response.getAnswer());

            // 대화 기록을 1개만 저장
            conversationHistory.getHistory().clear(); // 기존 기록 삭제
            conversationHistory.getHistory().add(newItem);

            // 대화 기록을 세션에 저장
            session.setAttribute("conversation_history", conversationHistory);

            // DB에 저장
            UUID user_id = userService.getCurrentUserUUID();
//            System.out.println("현재 사용자 ID: " + user_id);

            ConversationHistory history = new ConversationHistory();
            history.setUser_id(user_id);
            history.setConversation_question(userInput.getUser_question());
            history.setConversation_answer(response.getAnswer());
            history.setConversation_latitude(userInput.getLatitude());
            history.setConversation_longitude(userInput.getLongitude());
            history.setConversation_datetime(LocalDateTime.now());

            conversationHistoryService.saveConversationHistory(history);
            System.out.println("DB 저장 완료");

            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return new AiGuideResponse("API 요청 실패", null);
        }
    }



}