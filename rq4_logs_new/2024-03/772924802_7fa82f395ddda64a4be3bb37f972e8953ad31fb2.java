package telegrambot;

import bankClient.MonoBankExchangeRateClient;
import bankUtil.BankUtil;
import bankUtil.MonoBankUtil;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

import java.util.*;

import static telegrambot.BotConstants.BOT_NAME;
import static telegrambot.BotConstants.BOT_TOKEN;


public class CurrencyExchangeBot extends TelegramLongPollingBot {
    private BankUtil userBankSettings = null;
    private int numberAfterComa;

    @Override
        public String getBotUsername() {
            return BOT_NAME;
        }

        @Override
        public String getBotToken() {
            return BOT_TOKEN;
        }

        @Override
        public void onUpdateReceived(Update update) {
            Long chatId = getChatId(update);

            if (update.hasCallbackQuery()) {
                String callbackData = update.getCallbackQuery().getData();
                switch (callbackData) {
                    case "Settings":
                        sendSettingsMenu(chatId);
                        break;
                    case "DecimalPlaces":
                        sendDecimalPlacesMenu(chatId);
                        break;
                    case "Banks":
                        sendBanksMenu(chatId);
                        break;
                    case "Currencies":
                        sendCurrenciesMenu(chatId);
                        break;
                    case "AlertTime":
                        sendAlertTimeMenu(chatId);
                        break;
                    case "GetInfo":
                        sendInfo(chatId);
                        break;
                    case "Start" :
                        sendStartMenu(chatId);
                        break;
                    default:
                }
            } else {
                Map<String, String> buttons = new LinkedHashMap<>();
                buttons.put("Get Info", "GetInfo");
                buttons.put("Settings", "Settings");

                SendMessage message = new SendMessage();
                message.setText("Hello, glad to see you. This bot will help you track currency exchange rates.");
                attachButtons(message, buttons);
                message.setChatId(chatId);
                try {
                    execute(message);
                } catch (TelegramApiException e) {
                    e.printStackTrace();
                }
            }
        }

    private void sendInfo(Long chatId) {
        if(userBankSettings == null){
            userBankSettings = getDefaultSettings();
        }

        String usdInfo = userBankSettings.getUSD();

        SendMessage info = new SendMessage();
        info.setChatId(chatId);
        info.setText(usdInfo);
        attachButtons(info, Map.of(
                "Get Info", "GetInfo",
                "Settings", "Settings"));

        try {
            execute(info);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }

    }

    private BankUtil getDefaultSettings() {
        numberAfterComa = 2;
        MonoBankUtil monoBankUtil = new MonoBankUtil(numberAfterComa);
        monoBankUtil.setExchangeRates(new MonoBankExchangeRateClient().getMonoBankExchangeRates());
        return monoBankUtil;
    }

    private void sendStartMenu(Long chatId) {
        Map<String, String> buttons = new LinkedHashMap<>();
        buttons.put("Get Info", "GetInfo");
        buttons.put("Settings", "Settings");

        SendMessage startMessage = new SendMessage();
        startMessage.setText("Your changes have been saved.");
        attachButtons(startMessage, buttons);
        startMessage.setChatId(chatId);
        try {
            execute(startMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    private void sendSettingsMenu(Long chatId) {
        Map<String, String> buttons = new LinkedHashMap<>();
        buttons.put("Number of decimal places", "DecimalPlaces");
        buttons.put("Banks", "Banks");
        buttons.put("Currencies", "Currencies");
        buttons.put("Alert time", "AlertTime");
        buttons.put("Back", "Start");

        SendMessage settingsMessage = new SendMessage();
        settingsMessage.setText("Settings Menu");
        attachButtons(settingsMessage, buttons);
        settingsMessage.setChatId(chatId);
        try {
            execute(settingsMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    public void sendDecimalPlacesMenu(Long chatId) {
        Map<String, String> buttons = new LinkedHashMap<>();
        buttons.put("2", "2");
        buttons.put("3", "3");
        buttons.put("4", "4");
        buttons.put("Back", "Settings");

        SendMessage decimalPlacesMessage = new SendMessage();
        decimalPlacesMessage.setText("DecimalPlaces");
        attachButtons(decimalPlacesMessage, buttons);
        decimalPlacesMessage.setChatId(chatId);
        try {
            execute(decimalPlacesMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    private void sendBanksMenu(Long chatId) {
        Map<String, String> buttons = new LinkedHashMap<>();
        buttons.put("Monobank", "Monobank");
        buttons.put("PrivatBank", "PrivatBank");
        buttons.put("NBU", "NBU");
        buttons.put("Back", "Settings");

        SendMessage banksMessage = new SendMessage();
        banksMessage.setText("Choose bank:");
        attachButtons(banksMessage, buttons);
        banksMessage.setChatId(chatId);
        try {
            execute(banksMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    private void sendCurrenciesMenu(Long chatId) {
        Map<String, String> buttons = new LinkedHashMap<>();
        buttons.put("EUR", "EUR");
        buttons.put("USD", "USD");
        buttons.put("Back", "Settings");

        SendMessage currenciesMessage = new SendMessage();
        currenciesMessage.setText("Choose currency:");
        attachButtons(currenciesMessage, buttons);
        currenciesMessage.setChatId(chatId);
        try {
            execute(currenciesMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    public void sendAlertTimeMenu(Long chatId) {

        Map<String, String> buttons = new LinkedHashMap<>();
        buttons.put("9", "9");
        buttons.put("10", "10");
        buttons.put("11", "11");
        buttons.put("12", "12");
        buttons.put("13", "13");
        buttons.put("14", "14");
        buttons.put("15", "15");
        buttons.put("16", "16");
        buttons.put("17", "17");
        buttons.put("18", "18");
        buttons.put("19", "19");
        buttons.put("OFF", "OFF");
        buttons.put("Back", "Settings");

        SendMessage alertTimeMessage = new SendMessage();
        alertTimeMessage.setText("AlertTime:");
        attachButtons(alertTimeMessage, buttons);

        alertTimeMessage.setChatId(chatId);
        try {
            execute(alertTimeMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }



    public Long getChatId(Update update) {
        if (update.hasMessage()) {
            return update.getMessage().getChatId();
        } else if (update.hasCallbackQuery()) {
            return update.getCallbackQuery().getMessage().getChatId();
        }
        return null;
    }

    public void attachButtons(SendMessage message, Map<String, String> buttons) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
        for (String buttonName : buttons.keySet()) {
            String buttonValue = buttons.get(buttonName);
            InlineKeyboardButton button = new InlineKeyboardButton();
            button.setText(buttonName);
            button.setCallbackData(buttonValue);
            keyboard.add(Arrays.asList(button));
        }
        markup.setKeyboard(keyboard);
        message.setReplyMarkup(markup);
    }
}