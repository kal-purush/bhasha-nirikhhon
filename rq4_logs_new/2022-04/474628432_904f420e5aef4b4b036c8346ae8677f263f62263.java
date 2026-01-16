package com.example.registerstartupproject.registerAndEmailValidate;

import com.example.registerstartupproject.registerAndEmailValidate.DTO.ContactDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.mail.MessagingException;

@Service
@RequiredArgsConstructor
public class ContactService {
   private final EmailService emailService;


   void sendContactMessage (ContactDTO contactDTO)  {
      try
      {
         emailService.sendMail(contactDTO);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

}