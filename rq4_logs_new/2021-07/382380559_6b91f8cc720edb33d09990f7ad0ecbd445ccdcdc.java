package guru.qa;

import com.codeborne.selenide.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.codeborne.selenide.Selectors.byText;
import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.open;

public class TextBoxTests {
    @BeforeAll
    static void setup() {

        Configuration.baseUrl = "https://demoqa.com";
    }

    @Test
    void positiveFillTest() {
        open("/automation-practice-form");
        $("#firstName").setValue("Vasiliy");
        $("#lastName").setValue("Ivanov");
        $("#userEmail").setValue("vasya@gmail.uk");
        $("#genterWrapper").$(byText("Male")).click();
        $("#userNumber").setValue("0102030102");
        $("#dateOfBirthInput").click();
        $("select.react-datepicker__month-select").selectOption("August");
        $("select.react-datepicker__month-select").click();
        $("select.react-datepicker__year-select").selectOption("1992");
        $("select.react-datepicker__year-select").click();
        $("react-datepicker__day react-datepicker__day--021").click();
        $("#subjectsInput").setValue("QA testing");
        $("#hobbies-checkbox-1").$(byText("Sports")).click();
        $("#hobbies-checkbox-3").$(byText("Music")).click();
        $("#uploadPicture").click();
        $("#uploadPicture").val("C:\\Users\\Admin\\Desktop");
        $("#uploadPicture").click();
        $("#currentAddress").setValue("Lenina str, 22");
        $("#state").$(byText("Rajasthan"));
        $("#city").$(byText("Jaipur"));
        $("#submit").click();
    }
}