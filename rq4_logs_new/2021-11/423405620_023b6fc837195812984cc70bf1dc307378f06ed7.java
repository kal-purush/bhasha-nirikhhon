package ruangong.our_land.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * @Author: Lsutin
 * @Date: 2021/11/2 20:31
 * @describe:
 */
@Component
public class EmailUtil {

    @Autowired
    private JavaMailSenderImpl mailSender;
    @Value("${spring.mail.username}")
    private String sender;

    /**
     * 纯文本邮件发送
     * @param subject 邮件标题
     * @param text 正文
     * @param receiver 接收人邮箱
     */
    @Async
    public void sendPlainText(String subject, String text, String receiver) {
        // 创建发送对象
        SimpleMailMessage mailMessage = new SimpleMailMessage();
        // 设置邮件标题
        mailMessage.setSubject(subject);
        // 设置邮件正文
        mailMessage.setText(text);
        // 设置接收人
        mailMessage.setTo(receiver);
        // 设置发送人
        mailMessage.setFrom(sender);
        // 发送
        mailSender.send(mailMessage);
    }
}