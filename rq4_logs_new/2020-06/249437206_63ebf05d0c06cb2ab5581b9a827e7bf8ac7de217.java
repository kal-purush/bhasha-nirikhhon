package spring.app.core.steps;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import spring.app.core.BotContext;
import spring.app.exceptions.NoDataEnteredException;
import spring.app.exceptions.NoNumbersEnteredException;
import spring.app.exceptions.ProcessInputException;
import spring.app.model.Review;
import spring.app.model.User;
import spring.app.service.abstraction.ReviewService;
import spring.app.service.abstraction.StorageService;
import spring.app.service.abstraction.UserService;
import spring.app.service.abstraction.VkService;
import spring.app.util.StringParser;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static spring.app.core.StepSelector.*;
import static spring.app.util.Keyboards.DEF_BACK_KB;

@Component
public class UserStartChooseReview extends Step {
    private final ReviewService reviewService;
    private final UserService userService;
    private final StorageService storageService;
    private final VkService vkService;
    @Value("${review.point_for_empty_review}")
    private int pointForEmptyReview;

    public UserStartChooseReview(ReviewService reviewService, UserService userService,
                                 StorageService storageService, VkService vkService) {
        super("", DEF_BACK_KB);
        this.reviewService = reviewService;
        this.userService = userService;
        this.storageService = storageService;
        this.vkService = vkService;
    }

    @Override
    public void enter(BotContext context) {

    }

    @Override
    public void processInput(BotContext context) throws ProcessInputException, NoNumbersEnteredException, NoDataEnteredException {
        Integer vkId = context.getVkId();
        List<String> reviewIds = storageService.getUserStorage(vkId, USER_START_CHOOSE_REVIEW);
        String currentInput = context.getInput();
        if (currentInput.equalsIgnoreCase("назад")) {
            storageService.removeUserStorage(vkId, USER_START_CHOOSE_REVIEW);
            sendUserToNextStep(context, USER_MENU);
        } else if (StringParser.isNumeric(currentInput)) {
            int idNumber = Integer.parseInt(currentInput);
            if (idNumber > 0 && idNumber <= reviewIds.size()) {
                Long reviewId = Long.parseLong(reviewIds.get(idNumber - 1));
                Review reviewById = reviewService.getReviewById(reviewId);

                // то смотрим записан ли кто-то на него
                List<User> students = userService.getStudentsByReviewId(reviewId);
                if (!students.isEmpty()) {
                    // если кто-то записан на ревью, то сохраняем reviewId в STORAGE
                    storageService.updateUserStorage(vkId, USER_MENU, Arrays.asList(reviewId.toString()));
                    //теперь мы просто сохраняем юзеру его значения
                    sendUserToNextStep(context, USER_START_REVIEW_HANGOUTS_LINK);
                } else {
                    // если никто не записался на ревью, то добавялем очки пользователю и закрываем ревью
                    reviewById.setOpen(false);
                    reviewService.updateReview(reviewById);
                    User user = userService.getByVkId(vkId);
                    user.setReviewPoint(user.getReviewPoint() + pointForEmptyReview);
                    userService.updateUser(user);
                    vkService.sendMessage(
                            String.format("На твое ревью никто не записался, ты получаешь 1 RP.\nТвой баланс: %d RP", user.getReviewPoint()),
                            "",
                            vkId
                    );
                    sendUserToNextStep(context, USER_MENU);
                }
            } else {
                throw new ProcessInputException("Вы ввели некорректное число");
            }

        } else {
            throw new ProcessInputException("Вы ввели неверный формат числа");
        }
    }

    @Override
    public String getDynamicText(BotContext context) {
        Integer vkId = context.getVkId();
        List<String> reviewIds = storageService.getUserStorage(vkId, USER_START_CHOOSE_REVIEW);

        StringBuilder builder = new StringBuilder();
        builder.append(
                String.format(
                        "У вас имеется %d просроченных ревью. Выберите то, которое хотите начать:\n", reviewIds.size()
                )
        );
        IntStream.range(0, reviewIds.size())
                .forEach(i -> {
                    Review review = reviewService.getReviewById(Long.parseLong(reviewIds.get(i)));
                    String reviewStr = String.format(
                            "[%d] %s %s\n",
                            i + 1,
                            review.getTheme().getTitle(),
                            StringParser.localDateTimeToString(review.getDate())
                    );
                    builder.append(reviewStr);
                });
        return builder.toString();
    }

    @Override
    public String getDynamicKeyboard(BotContext context) {
        return "";
    }
}