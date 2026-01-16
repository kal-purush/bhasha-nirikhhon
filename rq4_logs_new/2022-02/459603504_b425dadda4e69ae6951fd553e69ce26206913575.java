package Pages;

import com.codeborne.selenide.SelenideElement;

import static com.codeborne.selenide.Selenide.$;

public class CartPage {

    public SelenideElement
                firstName = $("#input-payment-firstname"),
                lastName = $("#input-payment-lastname"),
                company = $("#input-payment-company"),
                address1 = $("#input-payment-address-1"),
                address2 = $("#input-payment-address-2"),
                city = $("#input-payment-city"),
                post = $("#input-payment-postcode"),
                country = $("#input-payment-country"),
                state = $("#input-payment-zone"),
                continueBtn = $("#button-payment-address");
}