package umgc.city.team1.utilities;

import org.simplejavamail.email.Email;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.MailerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import umgc.city.team1.models.outgoing.EmailInfo;
import umgc.city.team1.models.outgoing.EmailStatus;

public class SMTPEmailService implements EmailService {

    private final Logger logger = LoggerFactory.getLogger(EmailService.class);
    private final String host = "smtp.sendgrid.net";////TODO read from config
    private final int port = 587;////TODO read from config
    private final String username = "apikey";////TODO read from config
    private final String aPIKey = "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";//"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"; ////TODO Move to configuration

    @Override
    public boolean sendEmail(EmailInfo emailInfo) {
        //Send email
        EmailStatus emailStatus = sMTPEmailRelay(emailInfo);

        return emailStatus.isSent();
    }

    @Override
    public boolean sendEmail(String senderEmail, String recipientEmail, String subject, String body) {
        //Construct EmailInfo object and redirect to sendEmail(emailInfo)
        EmailInfo emailInfo = new EmailInfo();

        emailInfo.setSenderEmail(senderEmail);

        emailInfo.setRecipientEmail(recipientEmail);

        emailInfo.setSubject(subject);

        emailInfo.setBody(body);

        return sendEmail(emailInfo);
    }

    private EmailStatus sMTPEmailRelay(EmailInfo emailInfo) {
        var emailStatus = new EmailStatus();

        try {
            //Populate SMTP required parameters
            Email email = EmailBuilder.startingBlank()
                    .from(emailInfo.getSenderName(), emailInfo.getSenderEmail())
                    .to(emailInfo.getRecipientName(), emailInfo.getRecipientEmail())
                    .withSubject(emailInfo.getSubject())
                    .withPlainText(emailInfo.getBody())
                    .buildEmail();

            Mailer mailer = MailerBuilder
                    .withSMTPServer(host, port, username, aPIKey)
                    .buildMailer();

            mailer.sendMail(email);

            emailStatus.setSent(true);

        } catch (Exception ex) {
            logger.error(ex.getMessage());

            emailStatus.setSent(false);

            emailStatus.setErrorMessage("Emailed failed to send. Error code: " + ex.getMessage());
        }
        return emailStatus;
    }
}