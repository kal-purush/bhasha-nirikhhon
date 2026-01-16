package com.bin448.backend.service;

import com.bin448.backend.entity.ConfirmationToken;
import com.bin448.backend.entity.DTOentity.UserDTO;
import com.bin448.backend.entity.User;
import com.bin448.backend.repository.ConfirmationTokenRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.ModelAndView;

import java.util.UUID;

@Service
public class EmailInviteService {
    private JavaMailSender javaMailSender;
    private UserService us;
    private ConfirmationTokenRepository confirmationTokenRepository;

    @Autowired
    public EmailInviteService(JavaMailSender javaMailSender,UserService us,ConfirmationTokenRepository confirmationTokenRepository) {
        this.us = us;
        this.confirmationTokenRepository = confirmationTokenRepository;
        this.javaMailSender = javaMailSender;
    }

    @Async
    public void sendEmail(SimpleMailMessage email) {
        javaMailSender.send(email);
    }

    public boolean mailForInvitation(ModelAndView modelAndView, UserDTO user){

        User existingUser = us.getUserByUsername(user.getUsername());
        if(existingUser == null)
        {
            return false;
        }

        else
        {

            String uid = UUID.randomUUID().toString();
            SimpleMailMessage mailMessage = new SimpleMailMessage();
            mailMessage.setTo(user.getEmail());
            mailMessage.setSubject("You've been invited to travel with a friend!");
            mailMessage.setFrom("goodbyetravelagency.com");
            mailMessage.setText("Hi "+user.getName()+", to confirm your invitation, please click here : "
                    +"http://localhost:4200/#/invite"
            );

            sendEmail(mailMessage);

            modelAndView.addObject("emailId", user.getUsername());


            modelAndView.setViewName("successfulInvitation");

            return true;
        }



    }
    public ModelAndView accountConfimation(ModelAndView modelAndView, String tokenConf){

        UserDTO newUser = new UserDTO();
        String []parts = tokenConf.split("'");

        String Token =parts[0];
        newUser.setCity(parts[1]);
        newUser.setActive(true);
        newUser.setPassword(parts[5]);
        newUser.setRole("ROLE_USER");
        newUser.setEmail(parts[2]);
        newUser.setLastName(parts[3]);
        newUser.setName(parts[4]);
        newUser.setTelephone(parts[7]);
        newUser.setUsername(parts[8]);


        if(Token != null)
        {
            us.addUser(newUser);
            User baseUser = us.getUserByUsername(newUser.getUsername());
            ConfirmationToken ct = new ConfirmationToken(baseUser);
            confirmationTokenRepository.save(ct);
            modelAndView.setViewName("accountVerified");
        }
        else
        {
            modelAndView.addObject("message","The link is invalid or broken!");
            // modelAndView.setViewName("error");
        }

        return modelAndView;
    }
}