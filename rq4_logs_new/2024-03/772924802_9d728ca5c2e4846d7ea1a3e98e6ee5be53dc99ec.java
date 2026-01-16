package bankUtil;
import bankModel.NBU;

import java.util.ArrayList;
import java.util.List;

public class NBUUtil  implements BankUtil {
    private List<NBU> exchangeRates;
    private int numberAfterComa;
    public NBUUtil(int numberAfterComa){
        this.numberAfterComa = numberAfterComa;
        exchangeRates = new ArrayList<>();
    }

    public NBUUtil(List<NBU> nbuList){
        this.exchangeRates = nbuList;
    }
    @Override
    public void setReduction(int numberAfterComa) {
        numberAfterComa = numberAfterComa;
    }
    @Override
    public  String getUSD() {
        NBU nbu = new NBU();
        for (NBU element : exchangeRates) {
            if (element.getR030() == 840) {
                nbu = element;
            }
        }

        return String.format("course in Monobank USD/UAH\npurchase: %." + numberAfterComa + "f" +
                        "\nselling: %." + numberAfterComa + "f",
                nbu.getRate(), nbu.getRate());
    }
    @Override
    public  String getEUR() {
        NBU nbu = new NBU();
        for (NBU element : exchangeRates) {
            if (element.getR030() == 978) {
                nbu = element;
            }
        }
        return String.format("course in NBU EUR/UAH\npurchase: %."+numberAfterComa+"f" +
                        "\nselling: %."+numberAfterComa+"f",
                nbu.getRate(), nbu.getRate());
    }
}