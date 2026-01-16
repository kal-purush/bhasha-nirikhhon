package bankUtil;

import bankModel.PrivatBank;

import java.util.ArrayList;
import java.util.List;

public class PrivatBankUtil implements BankUtil {
    private int numberAfterComa;
    private List<PrivatBank> exchangeRates;

    public PrivatBankUtil(int numberAfterComa) {
        this.numberAfterComa = numberAfterComa;
        this.exchangeRates = new ArrayList<>();
    }

    @Override
    public void setReduction(int numberAfterComa) {
        this.numberAfterComa = numberAfterComa;
    }

    @Override
    public String getEUR() {
        if (exchangeRates.isEmpty()) {
            return "Exchange rates not available. Please fetch data first.";
        }

        StringBuilder result = new StringBuilder();
        for (PrivatBank bank : exchangeRates) {
            result.append("Course in PrivatBank EUR/UAH\n");
            result.append("purchase: ").append(String.format("%.2f", bank.getBuy())).append("\n");
            result.append("selling: ").append(String.format("%.2f", bank.getSale())).append("\n");
        }
        return result.toString();
    }

    @Override
    public String getUSD() {
        if (exchangeRates.isEmpty()) {
            return "Exchange rates not available. Please fetch data first.";
        }

        StringBuilder result = new StringBuilder();
        for (PrivatBank bank : exchangeRates) {
            result.append("Course in PrivatBank USD/UAH\n");
            result.append("purchase: ").append(String.format("%.2f", bank.getBuy())).append("\n");
            result.append("selling: ").append(String.format("%.2f", bank.getSale())).append("\n");
        }
        return result.toString();
    }
    public List<PrivatBank> getExchangeRates() {
        return exchangeRates;
    }

    public void setExchangeRates(List<PrivatBank> exchangeRates) {
        this.exchangeRates = exchangeRates;
    }
}