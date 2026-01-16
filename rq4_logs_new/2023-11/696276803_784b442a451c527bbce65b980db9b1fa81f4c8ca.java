import java.math.BigDecimal;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        BigDecimal tot = BigDecimal.ZERO;
        ArrayList<Double> arrayProva = new ArrayList<Double>();

        arrayProva.add(1d);
        arrayProva.add(5d);
        arrayProva.add(6d);
        arrayProva.add(1d);
        arrayProva.add(10d);
        arrayProva.add(15d);
        arrayProva.add(16d);



        for (Double numero : arrayProva) {
            tot = tot.add(new BigDecimal(numero));
        }

        System.out.println(tot);


    }

}