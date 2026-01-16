package currency.dto;

import currency.Bank;
import currency.Currency;
import currency.NotificationTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.List;
@Data
@Getter
@AllArgsConstructor
public class UserSettingsDTO{

    private Bank bank;

    private List<Currency> currency;

    private String precision;

    private NotificationTime Time;

    public UserSettingsDTO() {
        this.bank = Bank.PRIVATBANK;
        this. currency = List.of(Currency.USD);
        this.precision = "#.##";
        this.Time = NotificationTime.NO_TIME;

    }
}