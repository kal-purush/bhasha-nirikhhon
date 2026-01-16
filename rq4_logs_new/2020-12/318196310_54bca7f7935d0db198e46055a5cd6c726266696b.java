package be.upgrade.it.adventofcode2020.day10;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AdapterArray {
    private final List<Integer> adapters;

    public AdapterArray(String[] input) {
        this.adapters = Arrays.stream(input).map(Integer::parseInt).sorted().collect(Collectors.toList());
        this.adapters.add(0, 0);
        this.adapters.add(this.adapters.get(this.adapters.size()-1)+3);
    }

    public long getVoltage(){
        int single = 0;
        int triple = 0;

        for (int i = 1; i < adapters.size(); i++) {
            Integer previous = adapters.get(i-1);
            Integer current = adapters.get(i);
            int diff = current - previous;

            if(diff == 1){
                single++;
            }

            if(diff == 3){
                triple++;
            }
        }
        return single * triple;
    }

    public long getPossibleAdapterCombinations(){
        int pow2 = 0;
        int pow7 = 0;

        for (int i = 1; i < adapters.size()-1; i++) {
            long negative3 = (i >= 3) ? adapters.get(i - 3) : -9999;
            if (adapters.get(i + 1) - negative3 == 4)
            {
                pow7 += 1;
                pow2 -= 2;
            }
            else if (adapters.get(i + 1) - adapters.get(i - 1) == 2)
            {
                pow2 += 1;
            }

        }
        double p1 = Math.pow(2, pow2);
        double p2 = Math.pow(7, pow7);
        return (long) (p1*p2);

    }
}