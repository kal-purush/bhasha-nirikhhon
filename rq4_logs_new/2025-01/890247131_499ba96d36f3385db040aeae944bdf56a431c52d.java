package org.cyberrealm.tech.service;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "telegramClient", url = "${telegram.api.url}")
public interface TelegramClient {
    @PostMapping("/bot${telegram.bot.token}/sendMessage")
    void sendMessage(@RequestParam("chat_id") String chatId,
                     @RequestParam("text") String text);
}