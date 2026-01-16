package currency.impl;

import currency.Button;
import currency.Handlers;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.KeyboardButton;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.KeyboardRow;

import java.util.ArrayList;
import java.util.List;

public class BankButtonHandler implements Handlers {
    @Override
    public SendMessage sendMessage(Update update) {
        SendMessage message = new SendMessage();
        message.setText("Оберіть Банк:");
        message.setChatId(update.getCallbackQuery().getMessage().getChatId());

        List<KeyboardRow> keyboards = new ArrayList<>();
        KeyboardRow keyboardFirstRow = new KeyboardRow();
        keyboardFirstRow.add(new KeyboardButton(Button.BANK1.get()));
        keyboardFirstRow.add(new KeyboardButton(Button.BANK2.get()));
        keyboardFirstRow.add(new KeyboardButton(Button.BANK3.get()));

        keyboards.add(keyboardFirstRow);

        ReplyKeyboardMarkup rkm = new ReplyKeyboardMarkup();

        rkm.setKeyboard(keyboards);
        message.setReplyMarkup(rkm);

        return message;
    }
}