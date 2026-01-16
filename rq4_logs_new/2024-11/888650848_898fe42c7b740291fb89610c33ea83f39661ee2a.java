package service;

import model.Currency;
import utils.RoleValidateException;

import java.util.List;

public interface CurrencyService {

    Currency addCurrency(String currencyCode, String title);

    List<Currency> getAllCurrencies();

    boolean deleteCurrency(Currency currency) throws RoleValidateException;
}