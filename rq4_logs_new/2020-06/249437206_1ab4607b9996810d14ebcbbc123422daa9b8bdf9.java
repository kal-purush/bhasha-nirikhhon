package spring.app.core.steps;

import org.springframework.stereotype.Component;
import spring.app.core.BotContext;
import spring.app.core.StepHolder;
import spring.app.exceptions.ProcessInputException;
import spring.app.model.Review;
import spring.app.model.StudentReview;
import spring.app.model.User;
import spring.app.service.abstraction.*;
import spring.app.util.Keyboards;
import spring.app.util.StringParser;

import java.util.Arrays;
import java.util.List;

import static spring.app.core.StepSelector.*;
import static spring.app.util.Keyboards.*;

@Component
public class ReviewerDeleteReview extends Step {

    private final StorageService storageService;
    private final VkService vkService;
    private final ReviewService reviewService;
    private final ThemeService themeService;
    private final StudentReviewService studentReviewService;
    private final UserService userService;

    public ReviewerDeleteReview(VkService vkService, ReviewService reviewService, ThemeService themeService, StorageService storageService, StudentReviewService studentReviewService, UserService userService) {
        super("",CANCEL_OR_DELETE);//две кнопки: “отмена” и “да, отменить ревью”
        this.reviewService = reviewService;
        this.vkService = vkService;
        this.themeService = themeService;
        this.storageService = storageService;
        this.studentReviewService = studentReviewService;
        this.userService = userService;
    }

    @Override
    public void enter(BotContext context) {//здесь выводится текст в чате при переходе на этот step

    }

    @Override
    public void processInput(BotContext context) throws ProcessInputException {
        String command = context.getInput();
        if ("Да, отменить ревью".equals(command)) {
            long reviewId = Long.parseLong(storageService.getUserStorage(context.getVkId(), REVIEWER_DELETE_REVIEW).get(0));
            Review specReview = reviewService.getReviewById(reviewId);
            deleteReview(specReview, context);
            storageService.removeUserStorage(context.getVkId(), REVIEWER_DELETE_REVIEW);
            sendUserToNextStep(context, USER_MENU);
            StringBuilder message = new StringBuilder("Ревью {").append(themeService.getThemeById(specReview.getTheme().getId()).getTitle()).append("} - {").append(StringParser.localDateTimeToString(specReview.getDate())).append("} было успешно отменено.\n");
            storageService.updateUserStorage(context.getVkId(), USER_MENU, Arrays.asList((message.toString())));
        } else if ("Отмена".equals(command)) {
            sendUserToNextStep(context, SELECTING_REVIEW_TO_DELETE);
        } else {
            throw new ProcessInputException("Введена неверная команда...\n");
        }
    }

    private void deleteReview(Review review, BotContext context) {
        StepHolder stepHolder = context.getStepHolder();
        Integer vkId = context.getVkId();
        //всем участникам этого ревью, т.е. всем студентам, которые были на это ревью записаны - должно отправиться
        //сообщение с текстом: “Ревью {название темы} - {дата и время проведения} было отменено проверяющим”
        StringBuilder message = new StringBuilder("Ревью {").append(themeService.getThemeById(review.getTheme().getId()).getTitle()).append("} - {").append(StringParser.localDateTimeToString(review.getDate())).append("} было отменено проверяющим\n");
        List<User> studentsByReview = userService.getStudentsByReviewId(review.getId());
        for (User user : studentsByReview) {
            if (user.getChatStep() == USER_MENU) {
                //перед отправкой находящемуся на стэпе UserMenu студенту сообщения нужно определить, какие кнопки ему надо отображать
                StringBuilder keys = new StringBuilder(HEADER_FR);
                keys.append("[\n");
                keys.append(DEF_USER_MENU_KB);
                Integer usVkId = user.getVkId();
                // проверка, есть ли у юзера открытые ревью где он ревьюер - кнопка начать ревью
                List<Review> userReviews = reviewService.getOpenReviewsByReviewerVkId(usVkId);
                // проверка, записан ли он на другие ревью.
                Review studentReview = null;
                List<StudentReview> openStudentReview = studentReviewService.getOpenReviewByStudentVkId(usVkId);
                if (!openStudentReview.isEmpty()) {
                    if (openStudentReview.size() > 1) {
                        //TODO:впилить запись в логи - если у нас у студента 2 открытых ревью которые он сдает - это не нормально
                    }
                    studentReview = openStudentReview.get(0).getReview();
                }
                // формируем блок кнопок
                boolean isEmpty = true;//если не пустое - добавляем разделитель
                //кнопка начать ревью для ревьюера
                if (!userReviews.isEmpty()) {
                    keys
                            .append(ROW_DELIMETER_FR)
                            .append(REVIEW_START_FR)
                            .append(ROW_DELIMETER_FR)
                            .append(REVIEW_CANCEL_FR);
                    isEmpty = false;
                }
                //кнопка отмены ревью для студента
                if (studentReview != null) {
                    if (!isEmpty) {
                        keys.append(ROW_DELIMETER_FR);
                    }
                    keys.append(DELETE_STUDENT_REVIEW);
                }
                keys.append("]\n");
                keys.append(FOOTER_FR);
                //отправка каждому студенту сообщения message
                vkService.sendMessage(message.toString(), keys.toString(), usVkId);
            } else {
                Step userStep = stepHolder.getSteps().get(user.getChatStep());
                //отправка каждому студенту сообщения message
                vkService.sendMessage(message.toString(), userStep.getKeyboard(), user.getVkId());
            }
        }
        //удаление хранилища, связанного с USER_MENU
        storageService.removeUserStorage(vkId, USER_MENU);
        //собственно удаление самого ревью
        reviewService.deleteReviewById(review.getId());
    }

    @Override
    public String getDynamicText(BotContext context) {
        long reviewId = Long.parseLong(storageService.getUserStorage(context.getVkId(), REVIEWER_DELETE_REVIEW).get(0));
        String nameReview = themeService.getThemeById(reviewService.getReviewById(reviewId).getTheme().getId()).getTitle();
        String dateReview = StringParser.localDateTimeToString(reviewService.getReviewById(reviewId).getDate());
        StringBuilder message = new StringBuilder("Вы действительно хотите отменить ревью {").append(nameReview).append("} - {").append(dateReview).append("}?\n");
        return message.toString();
    }

    @Override
    public String getDynamicKeyboard(BotContext context) {
        return "";
    }
}