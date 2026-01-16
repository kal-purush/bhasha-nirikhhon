package bankUtil;

import bankModel.MonoBank;

import java.util.List;

public class MonoBankUtil implements BankUtil{
    private List<MonoBank> monoBankList;
    private int reduction;

    public MonoBankUtil(List<MonoBank> monoBankList){
        this.monoBankList = monoBankList;
    }
    @Override
    public void setReduction(int numberAfterComa) {
        reduction = numberAfterComa;
    }

    @Override
    public String getUSD() {
        MonoBank monoBank = new MonoBank();
        for (MonoBank element: monoBankList) {
            if(element.getCurrencyCodeA() == 840
                    && element.getCurrencyCodeB() == 980){
                monoBank = element;
            }
        }
        String a = "%";

        return String.format("course in Monobank USD/UAH\npurchase: %."+reduction+"f" +
                "\nselling: %."+reduction+"f",
                monoBank.getRateBuy(),monoBank.getRateSell());
    }

    @Override
    public String getEUR() {
        MonoBank monoBank = new MonoBank();
        for (MonoBank element: monoBankList) {
            if(element.getCurrencyCodeA() == 978
                    && element.getCurrencyCodeB() == 980){
                monoBank = element;
            }
        }
        String a = "%";

        return String.format("course in Monobank EUR/UAH\npurchase: %."+reduction+"f" +
                "\nselling: %."+reduction+"f",
                monoBank.getRateBuy(),monoBank.getRateSell());
    }
}