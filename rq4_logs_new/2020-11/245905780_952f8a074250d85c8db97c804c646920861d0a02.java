package hyve.petshow.controller.handler;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.MessageSource;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import hyve.petshow.domain.Conta;
import hyve.petshow.service.port.AcessoService;
import hyve.petshow.util.OnRegistrationCompleteEvent;

@Component
public class RegistrationListener implements ApplicationListener<OnRegistrationCompleteEvent> {
	@Value("${spring.app-url}")
	private String springAppUrl;
	
	@Autowired
	private AcessoService acessoService;
	@Autowired
	private MessageSource messages;
	@Autowired
	private JavaMailSender sender;
	
	
	@Override
	public void onApplicationEvent(OnRegistrationCompleteEvent event) {
		this.confirmRegistration(event);
	}

	private void confirmRegistration(OnRegistrationCompleteEvent event) {
		Conta conta = event.getConta();	
		String token = criaToken();
		acessoService.criaTokenVerificacao(conta, token);
		
		String emailDestino = conta.getEmail();
		String assunto = "Confirmação de cadastro";
		String urlConfirmacao = event.getAppUrl() + "/confirmacaoRegistro?token=" + token;
		String message = messages.getMessage("message.getSucc", null, event.getLocale());
		
		SimpleMailMessage email = new SimpleMailMessage();
		email.setTo(emailDestino);
		email.setSubject(assunto);
		email.setFrom("no-reply@petshow.com");
		email.setText(message + "\r\n" + springAppUrl + urlConfirmacao );
		sender.send(email);
		
	}
	
	public String criaToken() {
		return UUID.randomUUID().toString();
	}
}