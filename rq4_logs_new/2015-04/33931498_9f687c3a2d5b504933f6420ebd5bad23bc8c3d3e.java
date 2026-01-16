package twu.prime_factor;

import com.sun.javafx.scene.control.skin.VirtualFlow;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cmoers on 4/15/15.
 */
public class PrimeFactor {

    public String filterPrimeFactorsFor(int totalNumber){
        String result = "";
        List<Integer> primeFactors = getPrimeFactorsFor(totalNumber);

        for(Integer primeFactor : primeFactors){
            if(totalNumber % primeFactor == 0){
                result += primeFactor+",";
            }
        }
        return result.substring(0, result.length()-1);
    }

    private List<Integer> getPrimeFactorsFor(int totalNumber) {
        List<Integer> primeFactors = new ArrayList<>();

        for(int numberToPrint=2;numberToPrint<totalNumber;numberToPrint++) {
            int numberOfDivisions = 0;
            for(int numberToDivided=1; numberToDivided<=totalNumber; numberToDivided++ ){
               if(numberToPrint%numberToDivided == 0){
                   numberOfDivisions++;
                }
            }
            if(numberOfDivisions==2){
                primeFactors.add(numberToPrint);
            }
        }
        return primeFactors;
    }
}