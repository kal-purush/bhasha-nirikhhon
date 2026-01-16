package com.openlabs.shoppingmall.controller;

import com.openlabs.framework.dto.ResponseDto;
import com.openlabs.shoppingmall.dto.EmailMessage;
import com.openlabs.shoppingmall.service.EmailService;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/labshop/v1/email")
public class EmailController {
    private final EmailService emailService;

    @PostMapping("/emailTest")
    @ApiOperation(value = "이메일 전송 테스트")
    public ResponseDto<String> sendEmail(@RequestBody EmailMessage emailMessage) {
        emailService.sendMail(emailMessage);
        return ResponseDto.ok("success");
    }
}