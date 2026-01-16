package currency;

public enum Button {
    INFO("Отримати інфо"),
    SETTINGS("Налаштування"),
    NUMSIGNS("Кількість знаків після коми"),
    BANK("Банк"),
    CURRENCY("Валюти"),
    TIME("Час оповіщення"),
    SIGNS1("2"),
    SIGNS2("3"),
    SIGNS3("4"),
    BANK1("Приватбанк"),
    BANK2("Монобанк"),
    BANK3("НБУ"),
    TIME1("09-00"),
    TIME2("10-00"),
    TIME3("11-00"),
    TIME4("12-00"),
    TIME5("13-00"),
    TIME6("14-00"),
    TIME7("15-00"),
    TIME8("16-00"),
    TIME9("17-00"),
    TIME10("18-00"),
    NOTIME("Вимкнути повідомлення");


    private String value;
    public String get() {
        return value;
    }

    Button(String value) {
        this.value = value;
    }
}