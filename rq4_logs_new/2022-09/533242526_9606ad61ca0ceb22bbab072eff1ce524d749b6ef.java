package io.project.smartcontactmanager.controller;

import io.project.smartcontactmanager.service.EmailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpSession;
import java.util.Random;

@Controller
public class ForgotController {

    @Autowired
    private EmailService emailService;
    Random random = new Random(1000);

//    Email ID form Open handler
    @RequestMapping("/forgot")
    public String openEmailForm(){
        return "forgot_email_form";
    }

    @PostMapping("/send-otp")
    public String sendOTP(@RequestParam("email") String email, HttpSession session){

//        Generating OTP 4 digit

        System.out.println("EMAIL " + email);

        int otp = random.nextInt(999999);
        System.out.println("OTP " + otp);

//        Sending Mail
        String subject = "OTP From SCM";
        String message = "<h1> OTP = " + otp + "</h1>";
        String to = email;

        boolean flag = this.emailService.sendEmail(subject, message, to);

        if(flag){
            session.setAttribute("otp", otp);
            return "verify_otp";
        }
        else{
            session.setAttribute("message", "Check your EMAIL ID !!");

            return "forgot_email_form";
        }

    }
}