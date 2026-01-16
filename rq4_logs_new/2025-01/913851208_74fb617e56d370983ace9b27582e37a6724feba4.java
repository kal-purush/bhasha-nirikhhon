package com.project_merge.jigu_travel.api.ai_guide.fast;

import com.project_merge.jigu_travel.api.ai_guide.dto.AiGuideResponse;
import com.project_merge.jigu_travel.api.ai_guide.dto.UserInputRequestDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;

/**
 * FastApiClient
 * 호출 파라미터 : 음성파일, user_input(위도,경도,사용자 카테고리,이전 대화)
 * 리턴값 : GPT 답변, 이전 대화
 */
@FeignClient(name = "fastApiClient", url = "${ai-guide.api.host}") //perperites 등록
public interface FastApiClient {


    @PostMapping("/") //url 추후 수정 //음성 버전
    AiGuideResponse getAiGuide(@RequestPart("file") MultipartFile file, @RequestPart("userInput") UserInputRequestDto UserInputRequestDto);

    @PostMapping("/user_question") //음성파일 제외 텍스트로 테스트
    AiGuideResponse getAiGuideTest(@RequestBody UserInputRequestDto userInputRequestDto);
}