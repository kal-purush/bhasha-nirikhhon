package edu.java.bot.bot;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.UpdatesListener;
import com.pengrad.telegrambot.model.BotCommand;
import com.pengrad.telegrambot.request.SetMyCommands;
import edu.java.bot.comands.CommandInfo;
import edu.java.bot.configuration.ApplicationConfig;
import edu.java.bot.service.UserMessageProcessor;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BotInitializer {
    public final TelegramBot bot;

    @Autowired
    public BotInitializer(ApplicationConfig config, UserMessageProcessor botHandler) {
        bot = new TelegramBot(config.telegramToken());

        setMyCommands();

        bot.setUpdatesListener(updates -> {
            updates.forEach(update -> {
                if (update.message() != null && update.message().text() != null) {
                    bot.execute(botHandler.handleUpdate(update));
                }
            });
            return UpdatesListener.CONFIRMED_UPDATES_ALL;
        });
    }

    public void setMyCommands() {
        List<BotCommand> botCommands = new ArrayList<>();
        for (CommandInfo command : CommandInfo.values()) {
            botCommands.add(new BotCommand(command.getCommand(), command.getDescription()));
        }
        SetMyCommands setMyCommands = new SetMyCommands(botCommands.toArray(BotCommand[]::new));

        bot.execute(setMyCommands);
    }
}