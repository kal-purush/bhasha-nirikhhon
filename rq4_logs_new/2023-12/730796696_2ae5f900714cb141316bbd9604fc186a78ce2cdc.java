package currency.impl;

import currency.Button;
import currency.Handlers;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;

import java.util.Arrays;
import java.util.List;

public class MessageHandler implements Handlers {
    @Override
    public SendMessage sendMessage(Update update) {
        String receivedText = update.getMessage().getText();
        String text = "";
        List<String> buttonsTime = Arrays.asList(Button.TIME1.get(), Button.TIME2.get(), Button.TIME3.get(), Button.TIME4.get(), Button.TIME5.get(), Button.TIME6.get(), Button.TIME7.get(), Button.TIME8.get(), Button.TIME9.get(), Button.TIME10.get());
        SendMessage message = new SendMessage();
        if (receivedText.equals(Button.NOTIME.get())) {
            text = "Ви вимкнули час сповіщеннь, тому інформацію можете отримувати натискаючи кнопку Отримати інфо";
        }
        for (String button: buttonsTime) {
            if (receivedText.equals(button)) {
                text = "Ви обрали час надсилання повідомлень о " + receivedText;
            }
        }
        if (text.isEmpty()) {
            text = "Ви написали: " + receivedText + " Цей бот не має можливості опрацьовувати текст. Оберіть, будь ласка, кнопку";
        }
        message.setText(text);
        message.setChatId(update.getMessage().getChatId());

        return message;
    }
}