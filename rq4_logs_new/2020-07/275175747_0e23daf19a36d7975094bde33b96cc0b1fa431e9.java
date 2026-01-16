package mail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;


@RunWith(MockitoJUnitRunner.class)
public class SimpleEmailExampleControllerTest {
    @Mock
    public JavaMailSender emailSender;
    @Test
    public void sendSimpleEmail() {
        // Create a Simple MailMessage.
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo("java.mentor.jp@gmail.com");
        message.setSubject("Тема письма");
        message.setText("Текст письма");
        // Send Message!
        this.emailSender.send(message);
    }
}