package com.kamilmarnik.talkerr.mail;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Builder
public class MailSender {
  String userMail;
  String password;
  String userName;

  public void sendMail(String mailTo) {
    Properties prop = new Properties();
    prop.put("mail.smtp.host", "smtp.gmail.com");
    prop.put("mail.smtp.port", "587");
    prop.put("mail.smtp.auth", "true");
    prop.put("mail.smtp.starttls.enable", "true");

    Session session = Session.getInstance(prop,
        new javax.mail.Authenticator() {
          protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(userMail, password);
          }
        });

    try {
      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(userMail));
      message.setRecipients(
          Message.RecipientType.TO,
          InternetAddress.parse(mailTo)
      );
      message.setSubject("Talkerr says hello!");
      message.setText("Thank you for registration " + userName + "! You are now a part of Talkerr community");
      Transport.send(message);
    } catch (MessagingException e) {
      e.printStackTrace();
    }
  }
}