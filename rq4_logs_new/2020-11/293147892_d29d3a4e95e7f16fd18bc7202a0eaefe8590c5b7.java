package com.pnu.dev.shpaltaif.service.telegram;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.bots.TelegramWebhookBot;
import org.telegram.telegrambots.meta.api.methods.BotApiMethod;
import org.telegram.telegrambots.meta.api.objects.Update;

@Profile({"default", "test"})
@Service
public class TelegramBotStub extends TelegramWebhookBot implements SelfRegisteringTelegramBot, TelegramMessageSender {

    @Override
    public void registerWebhook() {

    }

    @Override
    public void sendMessageHtml(Long chatId, String content) {

    }

    @Override
    public BotApiMethod onWebhookUpdateReceived(Update update) {
        return null;
    }

    @Override
    public String getBotUsername() {
        return null;
    }

    @Override
    public String getBotToken() {
        return null;
    }

    @Override
    public String getBotPath() {
        return null;
    }
}