package site.wellmind.common.service;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import net.nurigo.java_sdk.api.Message;
import net.nurigo.java_sdk.exceptions.CoolsmsException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import site.wellmind.common.domain.vo.ExceptionStatus;
import site.wellmind.common.exception.GlobalException;
import site.wellmind.security.domain.model.SmsVerificationModel;
import site.wellmind.security.domain.vo.RequestStatus;
import site.wellmind.security.repository.SmsVerificationRepository;
import site.wellmind.security.util.EncryptionUtil;
import site.wellmind.user.repository.AdminTopRepository;
import site.wellmind.user.repository.UserTopRepository;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Random;

@Service
@RequiredArgsConstructor
public class CoolSmsService {

    @Value("${coolsms.api.key}")
    private String apiKey;

    @Value("${coolsms.api.secret}")
    private String apiSecret;

    @Value("${coolsms.api.number}")
    private String fromPhoneNumber;

    private final AdminTopRepository adminTopRepository;
    private final UserTopRepository userTopRepository;
    private final SmsVerificationRepository smsVerificationRepository;
    private final EncryptionUtil encryptionUtil;
    private final UtilService utilService;
    public Boolean sendSms(String phone) throws CoolsmsException {
        try {
            // 랜덤한 4자리 인증번호 생성
            String numStr = generateRandomNumber();

            String employeeId = adminTopRepository.findEmployeeIdByPhoneNum(phone)
                    .or(() -> userTopRepository.findEmployeeIdByPhoneNum(phone))
                    .orElseThrow(() -> new GlobalException(ExceptionStatus.ACCOUNT_NOT_FOUND));


            SmsVerificationModel smsVerificationModel = smsVerificationRepository.findFirstByPhoneNumOrderByRegDateDesc(phone);
            int requestCount = smsVerificationModel != null ? smsVerificationModel.getRequestCount() : 0;

            String encryptNum=encryptionUtil.encrypt(numStr);

            if (smsVerificationModel == null) {
                smsVerificationModel = SmsVerificationModel.builder()
                        .phoneNum(phone)
                        .employeeId(employeeId)
                        .verification(RequestStatus.N)
                        .requestCount(1)
                        .verifyKey(encryptNum)
                        .lastRequestTime(LocalDateTime.now())
                        .build();
            } else {
                smsVerificationModel.setVerifyKey(encryptNum);
                smsVerificationModel.setVerification(RequestStatus.N);
                smsVerificationModel.setRequestCount(requestCount + 1);
                smsVerificationModel.setLastRequestTime(LocalDateTime.now());
            }

            smsVerificationRepository.save(smsVerificationModel);


            Message coolsms = new Message(apiKey, apiSecret); // 생성자를 통해 API 키와 API 시크릿 전달

            HashMap<String, String> params = new HashMap<>();

            String formatPhoneNumber = utilService.formatPhoneNumber(phone);

            params.put("to", formatPhoneNumber);    // 수신 전화번호
            params.put("from", fromPhoneNumber);    // 발신 전화번호
            params.put("type", "sms");
            params.put("text", "\n[JSolution] 인증 안내\n\n안녕하세요.\n요청하신 인증번호는 [" + numStr + "]입니다.\n\n해당 인증번호는 10분 동안 유효합니다.\n감사합니다.");

            // 메시지 전송
            coolsms.send(params);

            return true; // 생성된 인증번호 반환

        } catch (Exception e) {
            throw new GlobalException(ExceptionStatus.INTERNAL_SERVER_ERROR, "Failed to send SMS"+e);
        }
    }

    // 랜덤한 4자리 숫자 생성 메서드
    private String generateRandomNumber() {
        Random rand = new Random();
        StringBuilder numStr = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            numStr.append(rand.nextInt(10));
        }
        return numStr.toString();
    }
}