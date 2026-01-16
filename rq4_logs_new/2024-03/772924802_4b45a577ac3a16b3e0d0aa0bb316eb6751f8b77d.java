package bankUtil;

import bankModel.PrivatBank;
import java.util.ArrayList;
import java.util.List;

public class PrivatBankUtil implements BankUtil {
    private List<PrivatBank> exchangeRates;
    private int numberAfterComa;

    public PrivatBankUtil(int numberAfterComa) {
        this.numberAfterComa = numberAfterComa;
        this.exchangeRates = new ArrayList<>();
    }

    @Override
    public void setReduction(int numberAfterComa) {
        this.numberAfterComa = numberAfterComa;
    }

    @Override
    public String getUSD() {
        PrivatBank privatBank = findCurrencyExchange("USD");
        if (privatBank == null) {
            return "Exchange rates not available. Please fetch data first.";
        }

        return String.format("Course in PrivatBank USD/UAH\npurchase: %." + numberAfterComa +
                        "f\nselling: %." + numberAfterComa + "f",
                Double.parseDouble(privatBank.getBuy()), Double.parseDouble(privatBank.getSale()));
    }

    @Override
    public String getEUR() {
        PrivatBank privatBank = findCurrencyExchange("EUR");
        if (privatBank == null) {
            return "Exchange rates not available. Please fetch data first.";
        }

        return String.format("Course in PrivatBank EUR/UAH\npurchase: %." + numberAfterComa +
                        "f\nselling: %." + numberAfterComa + "f",
                Double.parseDouble(privatBank.getBuy()), Double.parseDouble(privatBank.getSale()));
    }

    public void setExchangeRates(List<PrivatBank> exchangeRates) {
        this.exchangeRates = exchangeRates;
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