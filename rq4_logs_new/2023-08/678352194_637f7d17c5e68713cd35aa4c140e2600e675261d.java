package homeworks.homework4.ClientApplication;

import homeworks.homework4.Core.UserProvider;
import homeworks.homework4.Models.User;

/**
 * Класс аутентификации пользователя
 */
public class Authentication {
    /**
     * Метод производит аутентификацию
     */
    public static User authentication(UserProvider userProvider, String login, int passHash) {
        var client = userProvider.getClientByName( login );
        if (client.getHashPassword() == passHash) {
            return client;
        } else {
            throw new RuntimeException( "Authentication fail" );
        }
    }
}