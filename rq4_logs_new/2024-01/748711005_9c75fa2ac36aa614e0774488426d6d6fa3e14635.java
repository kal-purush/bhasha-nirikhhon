package com.example.demo.controller;


import com.example.demo.model.request.mail.VerifyCodeDto;
import com.example.demo.model.request.mail.VerifyRQ;
import com.example.demo.service.auth.AuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class VerifyCodeController {
    private final AuthService authService;
    @GetMapping("auth/verify")
    public String viewVerifyCode(ModelMap modelMap,
                                 VerifyCodeDto verifyCodeDto,
                                 @RequestParam String email){
        modelMap.addAttribute("verifyCodeDto",verifyCodeDto);
        modelMap.addAttribute("email",email);
        return "auth/verify-code";
    }
    @PostMapping("auth/verify")
    public String doVerifyCode(VerifyCodeDto verifyCodeDto,
                               @RequestParam String email){
        System.out.println(verifyCodeDto.get6Digits());
        String sixDigits = verifyCodeDto.get6Digits();

        authService.verifyRQ(new VerifyRQ(email,sixDigits));
        return "auth/verify-succeed";
    }
}