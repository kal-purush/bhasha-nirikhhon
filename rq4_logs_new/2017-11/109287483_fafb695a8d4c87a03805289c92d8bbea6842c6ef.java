package cooku;

import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;

public class Bot extends TelegramLongPollingBot {
	
	protected SendMessage messaggio = new SendMessage();	//crea un nuovo messaggio di tipo SendMessage;
	
	@Override
	public String getBotUsername() {
		return "INSERISCI_QUI_USERNAME";
	}
	
	@Override
	public String getBotToken() { //associa token bot
		return "INSERISCI_QUI_TOKEN";
	}

	@SuppressWarnings("deprecation")
	@Override
	public void onUpdateReceived(Update aggiornamento) {
		if (aggiornamento.hasMessage() && aggiornamento.getMessage().hasText()) {
	        	messaggio.setChatId(aggiornamento.getMessage().getChatId());
	        	System.out.println("Messaggio ricevuto da ID: "+aggiornamento.getMessage().getChatId()); //output di debug con id mittente
	        	try {
					messaggio.setText(Query.main(aggiornamento.getMessage().getText()));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	     
		
		try {
			sendMessage(messaggio);
			System.out.println("Risposta inviata!"); //output di debug di conferma risposta inviata
		} catch (TelegramApiException e) {
			e.printStackTrace();
		};
		
	};};

}