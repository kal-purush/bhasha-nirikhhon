package com.project.teamup.utils;

import com.project.teamup.dto.UserDTO;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

/**
 * Sends mails to different users of the application.
 * @author Sonya
 * @since 23.11.2019
 * */
public class MailService {

    /**
     * Sends an email
     *
     * @param userEmail email of the user who must receive the message
     * @param subject the subject of the mail
     * @param message the email's message
     * @throws MessagingException if the mail hasn't been sent
     * */
    public static void sendMail(String userEmail, String subject, String message) throws MessagingException {
        String username = "teamUp732@gmail.com";
        String password = "proiectcolectiv2019";
        Properties props = new Properties();
        props.put("mail.smtp.host", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");
        props.put("mail.smtp.auth", "true");
        //Establishing a session with required user details
        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
        MimeMessage msg = new MimeMessage(session);
        String to = userEmail;
        InternetAddress[] address = InternetAddress.parse(to, true);
        msg.setRecipients(Message.RecipientType.TO, address);
        msg.setFrom(new InternetAddress(username));
        msg.setSentDate(new Date());
        msg.setSubject(subject);
        msg.setText(message);
        msg.setHeader("XPriority", "1");
        Transport.send(msg);
    }

    /**
     * Builds the message of the mail that has to be
     * sent by an admin when he generates a new
     * password for a blocked user.
     *
     * @param blockedUser user whose password must
     *                    be generated
     * @param admin the admin who generates the password
     * @param newPassword the new generated password
     * @return the message of the mail
     * */
    public static String sendMessageNewPassword(UserDTO blockedUser, UserDTO admin, String newPassword){
        StringBuilder message = new StringBuilder();
        message.append("Hello, ");
        message.append(blockedUser.getFirstName());
        message.append(" ");
        message.append(blockedUser.getLastName());
        message.append("!\n\n");
        message.append("A new password has been set for you. You can now try to log in with" +
                " the following credentials: \n");
        message.append("Username: ");
        message.append(blockedUser.getUsername());
        message.append("\n");
        message.append("Password: ");
        message.append(newPassword);
        message.append("\n");
        message.append("I recommend you to log in and to change the new generated password immediately for security" +
                " reasons! If there are any further problems don't hesitate to write me.\n\n");
        message.append("Have a nice day,\n");
        message.append(admin.getFirstName());
        message.append(" ");
        message.append(admin.getLastName());
        return message.toString();
    }

    /**
     * @return the subject of the mail which must
     * be sent to an user when his password was
     * newly generated.
     * */
    public static String newPasswordSubject(){
        return "Recover your password";
    }

    /**
     * Builds the message of the email that has to be sent
     * to an admin when a user's account has been
     * deactivated to inform him that he has to
     * generate a new password for him.
     *
     * @param blockedUser the user whose password must
     *                    be generated
     * @param admin the admin who has to generate
     *              a new password for the user
     * @return the message of the mail
     * */
    public static String sendMessageDeactivatedAccount(UserDTO blockedUser, UserDTO admin){
        StringBuilder message = new StringBuilder();
        message.append("Hi, ");
        message.append(admin.getFirstName());
        message.append(" ");
        message.append(admin.getLastName());
        message.append("!\n\n");
        message.append("The account for the user with the following credentials has been deactivated: \n");
        message.append("Username: ");
        message.append(blockedUser.getUsername());
        message.append("Email: ");
        message.append(blockedUser.getEmail());
        message.append("\n");
        message.append("Please generate a new password and announce the blocked user.");
        return message.toString();
    }

    /**
     * @return the subject of the mail which must
     * be sent to an admin when a user's account
     * has been deactivated
     * */
    public static String accountDeactivatedSubject(){
        return "User deactivated";
    }
}