package controllers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import ninja.postoffice.Mail;
import ninja.postoffice.Postoffice;
import org.apache.commons.mail.EmailException;

import javax.mail.internet.AddressException;

/**
 * Created by zmi on 4/24/16.
 */
public class MailController {

    @Inject
    Provider<Mail> mailProvider;

    @Inject
    Postoffice postoffice;

    public void sendMail() {

        Mail mail = mailProvider.get();

        // fill the mail with content:
        mail.setSubject("Techies notification");

        mail.setFrom("notification@zhenximi.me");

        mail.addReplyTo("notification@zhenximi.me");

        mail.setCharset("utf-8");
        mail.addHeader("header1", "value1");
        mail.addHeader("header2", "value2");

        mail.addTo("zhenxi.mi@gmail.com");
        mail.addTo("oliviachenxi@gmail.com");
        mail.addTo("ls4162@nyu.edu");


//        mail.addCc("cc1@domain");
//        mail.addCc("cc2@domain");
//
//        mail.addBcc("bcc1@domain");
//        mail.addBcc("bcc2@domain");

        mail.setBodyHtml("test email from Techies");

        mail.setBodyText("Test bodyText: Techies want to be nerds and friends.");

        // finally send the mail
        try {
            postoffice.send(mail);
        } catch (EmailException | AddressException e) {
            // ...
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}