package bankUtil;

import bankModel.PrivatBank;

import java.util.List;

public class PrivatBankUtil implements BankUtil {
    private List<PrivatBank> exchangeRates;
    private int numberAfterComa;

    public PrivatBankUtil(int numberAfterComa) {
        this.numberAfterComa = numberAfterComa;
    }

    @Override
    public void setReduction(int numberAfterComa) {
        this.numberAfterComa = numberAfterComa;
    }

    @Override
    public String getUSD() {
        if (exchangeRates == null || exchangeRates.isEmpty()) {
            return "Exchange rates not available. Please fetch data first.";
        }

        PrivatBank privatBank = findCurrencyExchange("USD");
        if (privatBank == null) {
            return "USD exchange rate not available.";
        }

        return getFormattedExchangeRate(privatBank);
    }

    @Override
    public String getEUR() {
        if (exchangeRates == null || exchangeRates.isEmpty()) {
            return "Exchange rates not available. Please fetch data first.";
        }

        PrivatBank privatBank = findCurrencyExchange("EUR");
        if (privatBank == null) {
            return "EUR exchange rate not available.";
        }

        return getFormattedExchangeRate(privatBank);
    }

    public void setExchangeRates(List<PrivatBank> exchangeRates) {
        this.exchangeRates = exchangeRates;
    }

    public String getFormattedExchangeRate(PrivatBank bank) {
        if (bank == null) {
            return "Currency exchange information not available.";
        } else {
            return String.format("Course in PrivatBank %s/UAH\npurchase: %." + numberAfterComa + "f\nselling: %." + numberAfterComa + "f",
                    bank.getCcy(), Double.parseDouble(bank.getBuy()), Double.parseDouble(bank.getSale()));
        }
    }

    private PrivatBank findCurrencyExchange(String currency) {
        for (PrivatBank bank : exchangeRates) {
            if (bank.getCcy().equals(currency)) {
                return bank;
            }
        }
        return null;
    }
}