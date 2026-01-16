package pages;

import com.codeborne.selenide.Condition;

import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.open;

public class MainPage {

    String inputcss = "#twotabsearchtextbox";
    public void openPage(){ open("/");}
    public SearchResultsPage searchForText(String text){
        $(inputcss)
                .shouldBe(Condition.visible)
                .setValue(text).pressEnter();
        return new SearchResultsPage();
    }


}