package com.fatepet.petrest;

import com.fatepet.global.exception.SmsException;
import com.fatepet.global.response.ResponseCode;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.nurigo.sdk.NurigoApp;
import net.nurigo.sdk.message.exception.NurigoMessageNotReceivedException;
import net.nurigo.sdk.message.model.Message;
import net.nurigo.sdk.message.service.DefaultMessageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor
@Slf4j
public class SmsService {

    @Value("${coolsms.apikey}")
    private String apiKey;

    @Value("${coolsms.apisecret}")
    private String apiSecret;

    @Value("${coolsms.fromnumber}")
    private String fromNumber;

    private DefaultMessageService messageService;

    @PostConstruct
    private void init() {
        this.messageService = NurigoApp.INSTANCE.initialize(apiKey, apiSecret, "https://api.coolsms.co.kr");
    }

    public void sendSMS(String businessPhoneNumber, String customerPhoneNumber, String contactType, String inquiry) {
        String messageText = String.format(
                "페이트펫을 통한 상담 요청이 있습니다.\n" +
                        "현장결제 단계에서 최종 금액의 5%%할인을 적용부탁드립니다.\n\n" +
                        "연락처 : %s\n" +
                        "연락 방법: %s\n" +
                        "기타문의 사항 : %s",
                customerPhoneNumber,
                contactType,
                (inquiry != null && !inquiry.isBlank()) ? inquiry : "없음"
        );

        Message message = new Message();
        message.setFrom(fromNumber);
        message.setTo(businessPhoneNumber);
        message.setText(messageText);
        message.setSubject("페이트펫 상담 요청");

        try {
            messageService.send(message);
        } catch (NurigoMessageNotReceivedException e) {
            log.error("문자 발송 실패 목록={}", e.getFailedMessageList());
            log.error("오류 메세지={}", e.getMessage());
            throw new SmsException(ResponseCode.INTERNAL_ERROR);
        } catch (Exception e) {
            log.error("기타 오류 메세지={}", e.getMessage());
            throw new SmsException(ResponseCode.INTERNAL_ERROR);
        }
    }

}