package com.nexign.job.tests;

import org.junit.jupiter.api.*;

import static com.codeborne.selenide.Condition.text;
import static io.qameta.allure.Allure.step;


@DisplayName("Тесты на проверку содержимого главной страницы сайта job.nexign.com")
public class CheckMainPageContentTest extends TestBase {
    private final static String headerText = "Новые проекты\nВакансии\nСтудентам\nЦенности\nКорпоративная жизнь\nОфисы\nNexign Academy";
    private final static String mainText = "\n" +
            "\t\t\t\tБыть частью Nexign — значит каждый день бросать себе вызов, искать нестандартные решения, создавать технологии будущего\n" +
            "\t\t\t";

    @BeforeEach
    public void openCheckPage() {
        step("Открываем страницу job.nexign.com", () -> {
            mainPage.openPage();
        });
    }

    @Test
    @Tag("UITest")
    @DisplayName("Проверка содержимого заголовков в верхнем меню главной страницы")
    void menuItemTest() {
        step("Проверяем, что на странице есть заданный header", () -> {
            Assertions.assertTrue(mainPage.selectElementByName("header").exists(),
                    "Ошибка: на странице отсутствует ожидаемый header");
        });

        step("Проверяем, что header страницы содержит необходимые подзаголовки", () -> {
            mainPage.getHeaderSubs().shouldHave(text(headerText).because("Ошибка: в header нет ожидаемых пунктов меню"));
        });
    }

    @Test
    @Tag("UITest")
    @DisplayName("Проверка наличия блока контактов для связи")
    void selectionTextTest() {
        step("Проверяем, что на главной странице есть footer", () -> {
            Assertions.assertTrue(mainPage.selectElementByName("section").exists(),
                    "Ошибка: footer не найден");
        });

        mainPage.selectElementByName("section").scrollIntoView(true);

        step("Проверяем, что в нём есть раздел контакты", () -> {
            Assertions.assertTrue(mainPage.selectElementByText("Контакты").exists(),
                    "Ошибка: раздел контактов отсутствует или не имеет заголовка");
        });
    }

    @Test
    @Tag("UITest")
    @DisplayName("Проверка наличия имиджевого текста на первой странице сайта")
    void mainTextTest() {
        step("Проверяем, что на главной странице есть нужный текст", () -> {
            Assertions.assertTrue(mainText.equals(mainPage.getMainText().getOwnText()),
                    "Ошибка: неверный текст на главной странице");
        });
    }
}