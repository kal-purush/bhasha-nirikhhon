package spring.app.core.steps;

import org.springframework.stereotype.Component;
import spring.app.core.BotContext;
import spring.app.exceptions.NoDataEnteredException;
import spring.app.exceptions.NoNumbersEnteredException;
import spring.app.exceptions.ProcessInputException;
import spring.app.model.User;
import spring.app.service.abstraction.StorageService;

import java.util.Arrays;

import static spring.app.core.StepSelector.*;
import static spring.app.util.Keyboards.YES_NO_KB;

/**
 * @author AkiraRokudo on 30.05.2020 in one of sun day
 */
@Component
public class AdminConfirmSearch extends Step {

    @Override
    public void enter(BotContext context) {
        //Уточняем что это тот юзер, и выводим 2 кнопки - да и нет.
        String userIdString = context.getStorageService().getUserStorage(context.getVkId(), ADMIN_SEARCH).get(0);
        User user = context.getUserService().getUserById(Long.parseLong(userIdString));
        text = String.format("Найден пользователь %s %s (%s). Это нужный пользователь?", user.getFirstName(), user.getLastName(), user.getVkId());
        keyboard = YES_NO_KB;
    }

    @Override
    public void processInput(BotContext context) throws ProcessInputException, NoNumbersEnteredException, NoDataEnteredException {
        String input = context.getInput();
        StorageService storageService = context.getStorageService();
        Integer vkId = context.getVkId();
        if ("Нет".equals(input)) {
            nextStep = ADMIN_SEARCH;
            storageService.removeUserStorage(vkId, ADMIN_SEARCH);
        } else if ("Да".equals(input)) {
            String mode = storageService.getUserStorage(vkId, ADMIN_MENU).get(0);
            String findingUserId = storageService.getUserStorage(vkId, ADMIN_SEARCH).get(0);
            storageService.updateUserStorage(vkId, ADMIN_USERS_LIST, Arrays.asList(findingUserId));
            //определим куда мы пойдем с нашим юзером
            if ("delete".equals(mode)) {
                nextStep = ADMIN_REMOVE_USER;
            } else if ("edit".equals(mode)) {
                nextStep = ADMIN_EDIT_USER;
            } else {
                throw new ProcessInputException("Произошла ошибка при обработке ответа - вернитесь на предыдущий шаг выбрав вариант 'нет'");
            }
            //сюда мы не вернемся, так что очищаемся
            storageService.removeUserStorage(context.getVkId(), ADMIN_SEARCH);
        } else {
            throw new ProcessInputException("Введена неверная команда...");
        }
    }
}