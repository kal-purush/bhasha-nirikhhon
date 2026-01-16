package de.malkusch.ha.automation.presentation.chat;

import static java.util.Arrays.asList;

import org.springframework.context.annotation.Configuration;

import de.malkusch.telgrambot.Handler;
import de.malkusch.telgrambot.TelegramApi;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
class CommandConfiguration {

    private final Open open;
    private final TelegramApi telegram;

    @PostConstruct
    public void setup() {
        telegram.startDispatcher(asList( //
                new Handler.CallbackHandler(Open.COMMAND, open) //
        ));
    }
}