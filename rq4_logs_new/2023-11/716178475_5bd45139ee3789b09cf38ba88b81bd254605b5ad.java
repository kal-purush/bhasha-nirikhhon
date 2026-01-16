package com.se.fit.TravelProject.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;

@Service
public class SendMailService {
	@Autowired
	private JavaMailSender javaMailSender;

	private void sendEmail(String toEmail, String subject, String message) {
		MimeMessage mimeMessage = javaMailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(mimeMessage);

		try {
			helper.setTo(toEmail);
			helper.setSubject(subject);
			helper.setText(message);

			javaMailSender.send(mimeMessage);

			System.out.println("Email sent successfully!");
		} catch (MessagingException e) {
			e.printStackTrace();
		}
	}

	public void sendBookingConfirmationEmail(String toEmail) {
		String subject = "Booking Confirmation";
		String message = " TRAVEL.LỎ Thank you for booking! Your booking has been confirmed.";

		sendEmail(toEmail, subject, message);
	}
}