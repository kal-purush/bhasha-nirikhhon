/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mensa.sharewebservice.util;

import java.security.Security;
import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session; 
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 *
 * @author matt_
 */
public class EmailHandler {
    
    // <editor-fold desc="Testing Main">
    public static void main(String[] args) {
        EmailHandler.SendEmail("Testing Subject", "<h1>This is Matt Choy</h1>");
    }
    // </editor-fold>
    
    public static void SendEmail(String subject, String content) 
    {
        String sendTo = Common.getProperty(CommonEnum.PropertyKeys.EmailSendTo); 
        String sendFrom = Common.getProperty(CommonEnum.PropertyKeys.EmailSendFrom); 
        String host = Common.getProperty(CommonEnum.PropertyKeys.EmailHost); 
        final String user = Common.getProperty(CommonEnum.PropertyKeys.EmailUser); 
        final String password = Common.getProperty(CommonEnum.PropertyKeys.EmailPassword); 
        
        //Security.addProvider(new com.provider.JSSEProvider());
        Properties props = new Properties(); 
        props.setProperty("mail.transport.protocol", "smtp");   
        props.setProperty("mail.host", host); 
        props.put("mail.smtp.starttls.enable", "true");  
        props.put("mail.smtp.auth", "true");   
        props.put("mail.smtp.port", "587");   
        props.put("smtp.starttls.enable", "true");
        props.put("mail.smtp.socketFactory.fallback", "false");   
        props.setProperty("mail.smtp.quitwait", "false");   
        props.setProperty("mail.user", user);
        props.setProperty("mail.password", password);
        //Session session = Session.getDefaultInstance(props);  
        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user, password);
            }
        });
        try{
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(sendFrom));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(sendTo));
            message.setSubject(subject);
            message.setContent(content, "text/html" );
            //Transport trans = session.getTransport("smtp");
            //trans.connect(host, user, password);
            Transport.send(message);
            System.out.println("Sent message successfully....");
        }catch (MessagingException mex) {
            mex.printStackTrace();
        }
    }
}