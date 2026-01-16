package currency.dto;

import currency.Currency;
import currency.CurrencyItem;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class CurrencyItemNbuDto implements CurrencyItem {
    private int r030;
    private String txt;
    private double rate;
    private Currency cc;
    private String exchangedate;

    @Override
    public Double getSellRate() {
        return rate;
    }

    @Override
    public Double getBuyRate() {
        return rate;
    }
}