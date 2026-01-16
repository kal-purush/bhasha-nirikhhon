package com.shoppingcart.email;

import java.util.List;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.shoppingcart.dataUtil.ShoppingCartUtil;
import com.shoppingcart.dataUtil.UserDataUtil;
import com.shoppingcart.entity.CartProduct;

public class SendEmail {

	private String host = "smtp.gmail.com";
	private int port = 587;
	private String username = "avensys.training.cliftons@gmail.com";
	private String password = "Aven,123";
	private String sender = "avensys.training.cliftons@gmail.com";
	private ShoppingCartUtil shoppingCartUtil;
	private UserDataUtil userDataUtil;

	public SendEmail() {
		
	}

	public void sendMail(String username, String userEmail) throws Exception{
		
		int sno = 1;
		List<CartProduct> cartItems = shoppingCartUtil.getCartItems(username);		
		
		// Table details for the user's cart
		String cartHtml = "<table>"
				+ "<tr>"
				+ "<th>S/no.</th>"
				+ "<th>Product</th>"
				+ "<th>Quantity</th>"
				+ "</tr>";
		for(CartProduct a : cartItems) {
			cartHtml += "<tr>"
					+ "<td>" + sno + "</td>"
					+ "<td>" + a.getProduct().getProduct_name() + "</td>"
					+ "<td>" + a.getCart().getQuantity() + "</td>"
					+ "</tr>";
		}
		cartHtml += "</table>";
		
	
		Properties prop = new Properties();
		prop.put("mail.smtp.auth", true);
		prop.put("mail.smtp.starttls.enable", "true");
		prop.put("mail.smtp.host", host);
		prop.put("mail.smtp.port", port);
		prop.put("mail.smtp.ssl.trust", host);

		Session session = Session.getInstance(prop, new Authenticator() {

			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(sender));
			message.setRecipients(Message.RecipientType.TO,
					InternetAddress.parse(userEmail));
			message.setSubject("Avensys Shopping Cart - Purchase Receipt");

			String msg = "<!DOCTYPE html>"
					+ "<html>"
					+ "<body>"
					+ "<h1>Avensys Shopping Cart</h1>"
					+ "<br><br>"
					+ "<h2>" + userDataUtil.getUser(username) + " Cart Purchases</h2>"
					+ "<br><br>"
					+ cartHtml
					+ "</body>"
					+ "</html>";

			MimeBodyPart mimeBodyPart = new MimeBodyPart();
			mimeBodyPart.setContent(msg, "text/html");

			//MimeBodyPart attachmentBodyPart = new MimeBodyPart();
			//attachmentBodyPart.attachFile(new File("pom.xml"));

			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(mimeBodyPart);
			//multipart.addBodyPart(attachmentBodyPart);

			message.setContent(multipart);

			Transport.send(message);

			System.out.println("Email sent");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}