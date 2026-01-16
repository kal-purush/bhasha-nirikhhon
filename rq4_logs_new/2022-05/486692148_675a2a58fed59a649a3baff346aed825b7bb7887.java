package com.example.storeeverything.Email;


import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;



@Service
@AllArgsConstructor
public class EmailService implements EmailSender{


    private final JavaMailSender javaMailSender;
    private final static Logger LOGGER = LoggerFactory.getLogger(EmailService.class);



    @Override
    @Async
    public void send(String to, String email) {
        try{
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage,"utf-8");
            helper.setTo(to);
            helper.setSubject("CONFIRM YOUR EMAIL");
            helper.setFrom("praca.inzynierska.0022@gmail.com");
            helper.setText(email, true);
            javaMailSender.send(mimeMessage);
        }catch (MessagingException e){
            LOGGER.error("fail to send EMAIL", e);
            throw new IllegalStateException("fail to send EMAIL");
        }

    }
}