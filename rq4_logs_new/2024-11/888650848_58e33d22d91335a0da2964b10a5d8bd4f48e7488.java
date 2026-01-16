package repository;

import model.Currency;

import java.util.ArrayList;
import java.util.List;

public class CurrencyRepoImpl implements CurrencyRepository{

    private final List<Currency> currencies;


    public CurrencyRepoImpl() {
        this.currencies = new ArrayList<>();
        addCurrency();

    }

    private void addCurrency() {

        currencies.add(new Currency("USD", "Доллар США"));
        currencies.add(new Currency("EUR", "Евро"));
        currencies.add(new Currency("UAH", "Украинская гривна"));
        currencies.add(new Currency("PLN", "Польский злотый"));


    }
        @Override
    public Currency addCurrency(String currencyCode, String title) {

            Currency currency = new Currency( currencyCode, title);
            currencies.add(currency);
            return currency;
    }

    @Override
    public List<Currency> getAllCurrencies() {
        return currencies;
    }


    @Override
    public boolean deleteCurrency(Currency currency) {

        currencies.remove(currency);
        return true;
    }
}