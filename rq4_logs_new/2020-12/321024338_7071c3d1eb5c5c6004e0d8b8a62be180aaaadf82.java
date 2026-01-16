package rw.common;

import java.util.Properties;

import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import rw.member.model.vo.Member;


public class MailAuthentication {
	
	public int sendMail(String member){

	Properties p = new Properties(); // 정보를 담을 객체
	
	p.put("mail.smtp.host","smtp.gmail.com"); 
	p.put("mail.smtp.port", "465");
	p.put("mail.smtp.starttls.enable", "true");
	p.put("mail.smtp.auth", "true");
	p.put("mail.smtp.debug", "true");
	p.put("mail.smtp.socketFactory.port", "465");
	p.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
	p.put("mail.smtp.socketFactory.fallback", "false");

	try{
	    Authenticator auth = new SMTPAuthenticatior();
	    Session ses = Session.getInstance(p, auth);
	     
	    ses.setDebug(true);
	    MimeMessage msg = new MimeMessage(ses); // 메일의 내용을 담을 객체  

	    msg.setSubject("REVIEW:0127 인증 메일 입니다."); //  제목 

	    StringBuffer sb = new StringBuffer();
	    
	    //내용
	    sb.append("<div style='width:700px; height:400px;'>");
	    sb.append("<div style='width:100%; height:50px; background-color:#FFEB60'></div>"); 
	    sb.append("<div style='width:100%; text-align: center; font-size: 0.8rem'>");
	    sb.append("<h3>안녕하세요. REVIEW:0127입니다.</h3>");
	    sb.append("<h3> </h3>"); 
	    sb.append("이메일 인증을 위한 링크입니다.<br>");
	    sb.append("아래의 인증 링크를 클릭해주셔야 가입이 완료됩니다.<br>");
	    sb.append("<br><br><br><h3>인증 링크 : <a href='http://127.0.0.1/emailUpdate.rw?email="+member+"' method='post'>링크를 눌러서 이메일 인증을 완료해주세요.</a> </h3><br><br><br>");
	    sb.append("REVIEW:0127을 이용해주셔서 감사합니다.<br>");
	    sb.append("더 나은 서비스를 제공하기 위해 최선을 다하겠습니다.<br><br><br><b>본 메일은 발신전용 메일이므로 회신되지 않습니다.</b><b>기타 문의사항은 고객센터로 방문해주세요.</b>");
	    sb.append("</div><div style='width:100%; height:50px; background-color:#FFEB60'></div></div>");
	    
	    
		Address fromAddr = new InternetAddress("rvw0127@gmail.com");
		msg.setFrom(fromAddr);	

		Address toAddr = new InternetAddress(member);
		msg.addRecipient(Message.RecipientType.TO, toAddr); // 받는 사람 
		
		msg.setContent(sb.toString(), "text/html;charset=UTF-8"); // 내용
		Transport.send(msg); // 전송    

	} catch(Exception e){
	    e.printStackTrace();
	    System.out.println("발송 실패");
	    return 0;
	}	
	System.out.println("완료페이지로 이동");
	return 1;
	}
}