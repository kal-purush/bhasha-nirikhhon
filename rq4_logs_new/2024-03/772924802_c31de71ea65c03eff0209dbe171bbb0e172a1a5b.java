package alertTime;

import settings.UserSettings;
import telegrambot.CurrencyExchangeBot;

import java.time.LocalTime;
import java.util.HashMap;

public class AlertTime  implements Runnable{
    CurrencyExchangeBot bot;
    public AlertTime(CurrencyExchangeBot bot){
        this.bot = bot;
    }
    @Override
    public void run() {
        while (true) {
            LocalTime currentTime = LocalTime.now();
            int minute = currentTime.getMinute();

            if (minute > 0) {
                try {
                    Thread.sleep((long) (60 - minute) * 60000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }else {
                HashMap<Long, UserSettings> usersSettingsHashMap = bot.getUsersSettingsHashMap();

                for (Long chatId : usersSettingsHashMap.keySet()){
                    UserSettings userSettings = usersSettingsHashMap.get(chatId);
                    if(userSettings.getTime() > 0 && userSettings.getTime() == currentTime.getHour()) {
                        bot.sendInfo(chatId);
                    }
                }

                try {
                    Thread.sleep(3600000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}