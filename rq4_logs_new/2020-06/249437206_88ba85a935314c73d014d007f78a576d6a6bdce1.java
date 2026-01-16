package spring.app.core.steps;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import spring.app.core.BotContext;
import spring.app.exceptions.NoDataEnteredException;
import spring.app.exceptions.NoNumbersEnteredException;
import spring.app.exceptions.ProcessInputException;
import spring.app.util.StringParser;

@Component
public abstract class UserBaseFeedbackStep extends Step {

    @Value("${lower.bound}")
    private int lowBound;
    @Value("${upper.bound}")
    private int upBound;


    public UserBaseFeedbackStep() {
        super("", "");
    }

    @Override
    public void enter(BotContext context) {

    }

    @Override
    public void processInput(BotContext context) throws ProcessInputException, NoNumbersEnteredException, NoDataEnteredException {

        String currentInput = context.getInput();

        if (StringParser.isNumeric(currentInput)) {
            Integer userRating = Integer.parseInt(currentInput);
            // проверяем принадлежит ли число указанному интервалу
            if ((userRating >= lowBound) && (userRating <= upBound)) {
                ratingHandler(currentInput, context);
            } else {
                throw new NoNumbersEnteredException("Некорректный ввод, введите оценку в диапазоне от " + lowBound +
                        " до " + upBound + " числом!");
            }
        } else {
            throw new NoNumbersEnteredException("Введите числовое значение!");
        }
    }

    @Override
    public String getDynamicKeyboard(BotContext context) {
        return "";
    }

    /**
     * Возвращает строку со значениями границ
     * для оценки
     *
     * @return
     */
    public String getBoundsString() {
        return lowBound + " до " + upBound + "?";
    }

    /**
     * Метод записывает в StorageService оценки студента
     * его необходимо переопределить в каждом шаге
     *
     * @param input
     * @param context
     */
    public abstract void ratingHandler(String input, BotContext context);
}