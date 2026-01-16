package homework.never_use_switch;

import homework.never_use_switch.mail.MailInfo;
import homework.never_use_switch.mail.MailSender;

public class Main {
    public static void main(String[] args) {
        var welcomeMailInfo = MailInfo.builder().mailType(1).ownerName("Name 1").build();
        var happyBirthdayMailInfo = MailInfo.builder().mailType(2).ownerName("Name 2").build();


        var mailSender = new MailSender();
        mailSender.send(welcomeMailInfo);
        mailSender.send(happyBirthdayMailInfo);
    }
}