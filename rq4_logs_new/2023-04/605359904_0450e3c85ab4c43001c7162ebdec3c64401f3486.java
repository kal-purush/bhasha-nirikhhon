package smtp;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.*;

public class SendEmail {
   public static void sendEmail(String email, String name, String title, String content) {

      // Gmail 계정 정보 설정
//      final String username = "구글 계정 아이디";
//      final String password = "구글 계정 보안 비밀번호";
	 final String username = "네이버 계정 아이디";
     final String password = "네이버 계정 비밀번호";
      
      
      //위 final String password변수에 저장될 비밀번호 값은
      //보안 수준이 낮은 앱의 액세스를 활성화한 계정에서는 사용자 이름과 비밀번호를 사용해서 
      //Gmail SMTP와 같은 서드 파티 앱에 인증할 수 있었지만 
      //이제는 사용자의 계정을 더 안전하게 보호하기 위해서 
      //사용자 이름과 비밀번호를 사용해서 서드 파티 앱과 기기에 로그인 요청하는 것을 지원하지 않습니다. 
      //이제는 Gmail SMTP 서버를 이용하기 위해서는 보안 수준이 높은 Gmail 계정을 만들고나서 
      //사용자 이름과 비밀번호가 아닌 다른 방식(앱 비밀번호 를 생성해서 사용하는 방식)으로 인증을 요청해야만 합니다. 
      
      //참고
      //https://kdevkr.github.io/gmail-smtp/ 
           
      
      // 발신자 정보 설정
      String fromEmail = "본인 네이버 계정";
      String fromName = "Mood Movie";

      // 수신자 정보 설정
      String toEmail = email;
      String toName = name;

      // SMTP 서버 정보 설정
      String host = "smtp.naver.com";
//      String host = "smtp.google.com";
      String port = "587";

      // 이메일 제목 및 내용 설정
      String subject = title;
      String body = content;

      // Properties 설정
      Properties props = new Properties();
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.starttls.enable", "true");
      props.put("mail.smtp.host", host);
      props.put("mail.smtp.port", port);

      // Session 생성
      Session session = Session.getInstance(props, new javax.mail.Authenticator() {
         protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(username, password);
         }
      });

      try {
         // MimeMessage 생성
         MimeMessage message = new MimeMessage(session);
         message.setFrom(new InternetAddress(fromEmail, fromName));
         message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
         message.setSubject(subject);
         message.setText(body);

         // 이메일 전송
         Transport.send(message);

         System.out.println("이메일 전송 완료");

      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
