package uk.gov.hmcts.reform.workallocation.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.hmcts.reform.workallocation.model.Task;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

@Service
@Slf4j
public class EmailSendingService {

    @Value("${smtp.host}")
    private String smtpHost;

    @Value("${smtp.port}")
    private String smtpPort;

    @Value("${smtp.user}")
    private String smtpUser;

    @Value("${smtp.password}")
    private String smtpPassword;

    @Value("${smtp.from}")
    private String smtpFrom;

    @Value("${service.email}")
    private String serviceEmail;

    private final Session session;

    public EmailSendingService() {
        this.session = createSession();
    }

    public void sendEmail(Task task) throws Exception {
        log.info("Sending Email");

        MimeMessage msg = creatMimeMessage();
        msg.setReplyTo(InternetAddress.parse(smtpFrom, false));
        msg.setSubject(task.getState(), "UTF-8");
        StringBuilder builder = new StringBuilder();
        builder
            .append(task.getId())
            .append("\n")
            .append(task.getLastModifiedDate());
        msg.setText(builder.toString(), "UTF-8");

        msg.setSentDate(new Date());

        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(serviceEmail, false));
        Transport.send(msg);
        log.info("Email sending successful");
    }

    private MimeMessage creatMimeMessage() throws MessagingException {
        MimeMessage msg = new MimeMessage(this.session);
        msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
        msg.addHeader("format", "flowed");
        msg.addHeader("Content-Transfer-Encoding", "8bit");
        return msg;
    }


    private Session createSession() {
        Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.port", smtpPort);
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");

        Authenticator auth = new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(smtpUser, smtpPassword);
            }
        };
        return Session.getInstance(props, auth);
    }

}