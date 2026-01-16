package lesson_11.pages;

import com.codeborne.selenide.SelenideElement;
import io.qameta.allure.Step;

import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.open;

public class MainPage {

    private SelenideElement
            searchInput =$(".header-search-input");

    @Step
    public MainPage openPage() {
        open("https://github.com/");
        return this;
    }

    @Step
    public MainPage clickSearch() {
        searchInput.click();
        return this;
    }

    @Step
    public MainPage setSearch(String str) {
        searchInput.sendKeys(str);
        return this;
    }

    @Step
    public MainPage submitSearch() {
        searchInput.submit();
        return this;
    }
}