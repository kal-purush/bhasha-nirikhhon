package com.unifucamp.gamearena.service;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

    private static final Logger log = LoggerFactory.getLogger(EmailService.class);

    private final JavaMailSender mailSender;

    @Autowired
    public EmailService(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    public void enviarConfirmacaoPagamento(String destinatario, String nickname) {
        try {
            MimeMessage mensagem = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mensagem, true, "UTF-8");

            helper.setTo(destinatario);
            helper.setSubject("🎮 Pagamento Confirmado - Unifucamp GameArena");
            helper.setText(gerarCorpoHtml(nickname), true); // true = HTML

            mailSender.send(mensagem);

            log.info("Email enviado com sucesso! E-mail: {}, Nickname: {}", destinatario, nickname);

        } catch (MessagingException e) {
            throw new RuntimeException("Erro ao enviar e-mail de confirmação de pagamento", e);
        }
    }

    private String gerarCorpoHtml(String nome) {
        return """
            <html>
                <body style="font-family: Arial, sans-serif; background-color: #f7f7f7; padding: 20px;">
                    <div style="max-width: 600px; margin: auto; background-color: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0px 0px 10px rgba(0,0,0,0.1);">
                        <h2 style="color: #4CAF50;">✅ Pagamento Confirmado!</h2>
                        <p>Olá <strong>%s</strong>,</p>
                        <p>Seu pagamento foi <strong>confirmado com sucesso</strong>!</p>
                        <p>Agora você está oficialmente inscrito na <strong>Unifucamp GameArena</strong>! 🎉</p>
                        <hr>
                        <p style="font-size: 12px; color: #777;">Este é um e-mail automático. Por favor, não responda.</p>
                    </div>
                </body>
            </html>
        """.formatted(nome);
    }
}