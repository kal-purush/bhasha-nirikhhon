package com.xianyo.util.service;

import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;
public class PostmanService {
    // 发件人的 邮箱 和 密码（替换为自己的邮箱和密码）
    // PS: 某些邮箱服务器为了增加邮箱本身密码的安全性，给 SMTP 客户端设置了独立密码（有的邮箱称为“授权码”）,
    //     对于开启了独立密码的邮箱, 这里的邮箱密码必需使用这个独立密码（授权码）。
    private final String myEmailAccount = "213150735@seu.edu.cn";
    private final String myEmailPassword = "he727421454";
    // 发件人邮箱的 SMTP 服务器地址, 必须准确, 不同邮件服务器地址不同, 一般(只是一般, 绝非绝对)格式为: smtp.xxx.com
    private final String myEmailSMTPHost = "smtp.seu.edu.cn";
    private Properties props;
    //    private final String accountConfirmURL="<a href=\"http://localhost:8080/userAccountConfirm.register\"/>";
    private final String accountConfirmURL = "http://localhost:8080/userAccountConfirm.register";

    public void SendEmail(String aimEmailAddress) {
        try {
            // 1. 创建参数配置, 用于连接邮件服务器的参数配置
            props = new Properties();                    // 参数配置
            props.setProperty("mail.transport.protocol", "smtp");   // 使用的协议（JavaMail规范要求）
            props.setProperty("mail.smtp.host", myEmailSMTPHost);   // 发件人的邮箱的 SMTP 服务器地址
            props.setProperty("mail.smtp.auth", "true");            // 需要请求认证
            // 2. 根据配置创建会话对象, 用于和邮件服务器交互
            Session session = Session.getDefaultInstance(props);
            session.setDebug(true);                                 // 设置为debug模式, 可以查看详细的发送 log
            // 3. 创建一封邮件
            MimeMessage message = CreateMessage(session, myEmailAccount, aimEmailAddress);
            // 4. 根据 Session 获取邮件传输对象
            Transport transport = session.getTransport();
            // 5. 使用 邮箱账号 和 密码 连接邮件服务器,
            transport.connect(myEmailAccount, myEmailPassword);
            // 6. 发送邮件, 发到所有的收件地址, message.getAllRecipients() 获取到的是在创建邮件对象时添加的所有收件人, 抄送人, 密送人
            transport.sendMessage(message, message.getAllRecipients());
            // 7. 关闭连接
            transport.close();
            transport = session.getTransport();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一封只包含文本的简单邮件
     *
     * @param session     和服务器交互的会话
     * @param sendMail    发件人邮箱
     * @param receiveMail 收件人邮箱
     * @return
     * @throws Exception
     */
    private MimeMessage CreateMessage(
            Session session, String sendMail, String receiveMail) throws Exception {
        // 1. 创建一封邮件
        MimeMessage message = new MimeMessage(session);
        // 2. From: 发件人（昵称有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改昵称）
        message.setFrom(new InternetAddress(sendMail, "XianYo", "UTF-8"));
        // 3. To: 收件人（可以增加多个收件人、抄送、密送）
        message.setRecipient(MimeMessage.RecipientType.TO,
                new InternetAddress(receiveMail, "XX用户", "UTF-8"));
        // 4. Subject: 邮件主题（标题有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改标题）
        message.setSubject("猜猜我是谁", "UTF-8");
        // 5. Content: 邮件正文（可以使用html标签）
        message.setContent("请登录" + accountConfirmURL + " 进行密码确认", "text/html;charset=UTF-8");
        // 6. 设置发件时间
        message.setSentDate(new Date());
        // 7. 保存设置
        message.saveChanges();
        return message;
    }
}