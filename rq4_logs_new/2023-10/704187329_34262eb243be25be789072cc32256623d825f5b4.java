package br.com.serratec.ecommerce.model.email;

import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;

public class HtmlEmail {

    public static void main(String[] args) {
        // Configurações do servidor de email
        String host = "smtp.example.com";
        String username = "your_username";
        String password = "your_password";

        // Propriedades do sistema
        Properties properties = System.getProperties();
        properties.setProperty("mail.smtp.host", host);
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.port", "587"); // Porta do servidor SMTP

        // Sessão de email
        Session session = Session.getDefaultInstance(properties, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        try {
            // Criar uma mensagem
            MimeMessage message = new MimeMessage(session);

            // Configurar remetente e destinatário
            message.setFrom(new InternetAddress("your_email@example.com"));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress("recipient@example.com"));
            message.setSubject("Assunto do Email");

            // Conteúdo HTML do e-mail (definido como uma string)
            String htmlContent = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\r\n" + //
                    "<html xmlns=\"http://www.w3.org/1999/xhtml\">\r\n" + //
                    "<head>\r\n" + //
                    "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\" />\r\n" + //
                    "    <title>G5 E-commerce</title>\r\n" + //
                    "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />\r\n" + //
                    "</head>\r\n" + //
                    "<body style=\"margin: 0; padding: 0;\">\r\n" + //
                    "    <table align=\"center\" border=\"0\" cellpadding=\"0\" cellspacing=\"0\" width=\"600\">\r\n" + //
                    "        <tr>\r\n" + //
                    "            <td align=\"center\" bgcolor=\"#70bbd9\" style=\"padding: 40px 0 30px 0;\" height=\"230\">\r\n" + //
                    "                <h1 style=\"color: #fff; font-size: 24px; margin: 0;\">Grupo 5 E-Commerce</h1>\r\n" + //
                    "            </td>\r\n" + //
                    "        </tr>\r\n" + //
                    "        <tr>\r\n" + //
                    "            <td bgcolor=\"#87CEFA\" style=\"padding: 40px 30px 40px 30px;\">\r\n" + //
                    "                <table border=\"1\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">\r\n" + //
                    "                    <tr>\r\n" + //
                    "                        <td>\r\n" + //
                    "                            Olá Usuário,\r\n" + //
                    "                        </td>\r\n" + //
                    "                    </tr>\r\n" + //
                    "                    <tr>\r\n" + //
                    "                        <td>\r\n" + //
                    "                            Seu pedido:\r\n" + //
                    "                        </td>\r\n" + //
                    "                    </tr>\r\n" + //
                    "                </table>\r\n" + //
                    "                <table border=\"1\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">\r\n" + //
                    "                    <tr>\r\n" + //
                    "                        <td width=\"260\" valign=\"top\">\r\n" + //
                    "                            <table border=\"1\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">\r\n" + //
                    "                                <tr>\r\n" + //
                    "                                    <td width=\"100%\" height=\"140\" style=\"display: block;\">\r\n" + //
                    "                                        Itens do pedido\r\n" + //
                    "                                    </td>\r\n" + //
                    "                                </tr>\r\n" + //
                    "                                <tr>\r\n" + //
                    "                                    <td style=\"padding: 25px 0 0 0;\">\r\n" + //
                    "                                        Total de Itens\r\n" + //
                    "                                    </td>\r\n" + //
                    "                                </tr>\r\n" + //
                    "                            </table>\r\n" + //
                    "                        </td>\r\n" + //
                    "                        <td style=\"font-size: 0; line-height: 0;\" width=\"20\">\r\n" + //
                    "                            &nbsp;\r\n" + //
                    "                        </td>\r\n" + //
                    "                        <td width=\"260\" valign=\"top\">\r\n" + //
                    "                            <table border=\"1\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">\r\n" + //
                    "                                <tr>\r\n" + //
                    "                                    <td width=\"100%\" height=\"140\" style=\"display: block;\">\r\n" + //
                    "                                        Valor Unitário\r\n" + //
                    "                                    </td>\r\n" + //
                    "                                </tr>\r\n" + //
                    "                                <tr>\r\n" + //
                    "                                    <td style=\"padding: 25px 0 0 0;\">\r\n" + //
                    "                                        Valor Total\r\n" + //
                    "                                    </td>\r\n" + //
                    "                                </tr>\r\n" + //
                    "                            </table>\r\n" + //
                    "                        </td>\r\n" + //
                    "                    </tr>\r\n" + //
                    "                </table>\r\n" + //
                    "            </td>\r\n" + //
                    "        </tr>\r\n" + //
                    "        <tr>\r\n" + //
                    "            <td bgcolor=\"#B0C4DE\" style=\"padding: 30px 30px 30px 30px;\">\r\n" + //
                    "                <table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">\r\n" + //
                    "                    <tr>\r\n" + //
                    "                        <td width=\"75%\">\r\n" + //
                    "                            Grupo 5 Enterprises\r\n" + //
                    "                        </td>\r\n" + //
                    "                        <td align=\"right\">\r\n" + //
                    "                            <table border=\"0\" cellpadding=\"0\" cellspacing=\"0\">\r\n" + //
                    "                                <tr>\r\n" + //
                    "                                    <td>\r\n" + //
                    "                                        <a href=\"http://www.twitter.com/\" style=\"text-decoration: none; color: #000;\">X</a>\r\n" + //
                    "                                    </td>\r\n" + //
                    "                                    <td style=\"font-size: 0; line-height: 0;\" width=\"20\">&nbsp;</td>\r\n" + //
                    "                                    <td>\r\n" + //
                    "                                        <a href=\"http://www.espn.com.br/\" style=\"text-decoration: none; color: #000;\">ESPN</a>\r\n" + //
                    "                                    </td>\r\n" + //
                    "                                </tr>\r\n" + //
                    "                            </table>\r\n" + //
                    "                        </td>\r\n" + //
                    "                    </tr>\r\n" + //
                    "                </table>\r\n" + //
                    "            </td>\r\n" + //
                    "        </tr>\r\n" + //
                    "    </table>\r\n" + //
                    "</body>\r\n" + //
                    "</html>\r\n" + //
                    ""; // Substitua pelo seu HTML

            // Crie uma parte do email para o conteúdo HTML
            MimeBodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setContent(htmlContent, "text/html");

            // Crie o corpo do email
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);

            // Defina o corpo do email
            message.setContent(multipart);

            // Enviar o email
            Transport.send(message);
            System.out.println("Email HTML enviado com sucesso!");
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
    }
}