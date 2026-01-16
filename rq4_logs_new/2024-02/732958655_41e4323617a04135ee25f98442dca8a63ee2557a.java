package com.challenger.fridge.controller;



import com.challenger.fridge.dto.notice.DeviceTokenRequest;
import com.challenger.fridge.service.NoticeService;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
@RequestMapping("/notice")
public class NoticeController {
    private final NoticeService noticeService;

    /**
     * 이 API는 디바이스 토큰을 저장하기 위한 API 이 토큰은 회원가입 할 때 저장을 하거나 이 후에 따로 
     * 푸쉬 알림 설정하는 부분에서 해당 프론트가 FCM에게 해당 토큰을 요청해서 백엔드 서버에 토큰 저장을 
     * 요청해야함 추후 고려
     * @param deviceTokenRequest
     */
    @PostMapping("/fcm/token/")
    @Operation(summary = "사용자의 디바이스 토큰 API", description = "FCM에 접근하기 위해서 토큰을 저장해 두어야한다.")
    public void createDeviceToken(@RequestBody DeviceTokenRequest deviceTokenRequest)
    {

    }



}