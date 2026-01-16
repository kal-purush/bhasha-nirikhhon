package com.klb.commons;

import com.klb.exceptions.IncorrectArgumenException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

/**
 * Created by klb on 25.08.18.
 */
public class NumbersCommons {
    public static Double minPositive(List<Double> list) {
        Double minPositive = Double.MAX_VALUE;

        for(int i = 0; i < list.size(); i++) {
            Double element = list.get(i);
            if(element > 0 &&  element< minPositive) {
                minPositive = element;
            }
        }

        //TODO: rzucanie wyjatku gdy wszystkie elementy sa ujemne

        return minPositive;
    }

    public static Double sum(List<Double> list, int begin, int end) throws IncorrectArgumenException {
        if(begin < 0 || end >= list.size()) {
            throw new IncorrectArgumenException();
        }

        Double sum = 0.0;
        for(int i = begin; i <= end; i++) {
            sum+=list.get(i);
        }

        return sum;
    }
}