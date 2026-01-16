package beans;

import java.util.Properties;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import dal.tblMusteriDAO;
import models.tblmusteri;

@ManagedBean
@SessionScoped
public class sifremiunuttumBean {

	private String email;
	private tblmusteri bulunan=new tblmusteri();
	tblMusteriDAO db = new tblMusteriDAO();
	public void gonder() {
		for (tblmusteri item : db.listA(new tblmusteri())) {
			if(email.equals(item.getEmail())){
				bulunan = item;
				mailyolla();
			}
		}
	}
	
	

	public void mailyolla() {
	
		final String username = "vektoreljavaali@gmail.com";
		final String password = "Vektorel2004";

		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(email));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("muhammedali55@gmail.com"));
			message.setSubject("�yelik Aktivasyonu");
			message.setText("Merhabe Yeni �ye," + "\n\n "
							+"�ye Ad�n�z...: "+bulunan.getAd()
							+"�ifreniz.....: "+bulunan.getSifre()+
							"�yeli�ini Aktif Etmek i�in Link e T�kla...: \n\n"
							//"http://localhost:8080/MusteriTakip/aktivasyon.jsf?kod="+kodver()
					);

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
}


	
	
	
	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	
	
}