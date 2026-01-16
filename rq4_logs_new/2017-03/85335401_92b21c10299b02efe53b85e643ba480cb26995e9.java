import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.TelegramBotsApi;
import org.telegram.telegrambots.exceptions.TelegramApiException;

/**
 * @author Dmitry Tarasov
 *         Date: 03/17/2017
 *         Time: 19:58
 */
public class Main {
        public static void main(String[] args) {

            ApiContextInitializer.init();

            TelegramBotsApi telegramBotsApi = new TelegramBotsApi();
            try {
                telegramBotsApi.registerBot(new ToDoListBot());
            } catch (TelegramApiException e) {
                e.printStackTrace();
            }
        }
}