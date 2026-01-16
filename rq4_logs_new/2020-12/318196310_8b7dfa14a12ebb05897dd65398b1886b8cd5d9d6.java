package be.upgrade.it.adventofcode2020.day9;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Encoder {
    private final List<Long> numbers;
    private final int preambleLength;

    public Encoder(int preambleLength, String[] lines) {
        this.numbers = Arrays.stream(lines).map(Long::parseLong).collect(Collectors.toList());
        this.preambleLength = preambleLength;
    }

    public long findFirstEncodingError(){
        for (int i = preambleLength; i < numbers.size(); i++) {
            if(!isSumOfTwoPreambleValues(i)){
                return numbers.get(i);
            }
        }
        return -1L;
    }

    public long findEncryptionWeakness(){
        for (int i = preambleLength; i < numbers.size(); i++) {
            if(!isSumOfTwoPreambleValues(i)){
                Long error = numbers.get(i);
                List<Long> previous = numbers.subList(0, i);
                for (int j = 0; j < previous.size() - 1; j++) {
                    long count = 0;
                    List<Long> contigous = new ArrayList<>();
                    for (int k = 0; count < error; k++) {
                        count+= previous.get(j+k);
                        contigous.add(previous.get(j+k));
                    }
                    if(count == error){
                        List<Long> ordered = contigous.stream().sorted().collect(Collectors.toList());
                        return ordered.get(0)+ordered.get(ordered.size()-1);
                    }
                }
            }
        }
        return -1L;
    }

    private boolean isSumOfTwoPreambleValues(int index) {
        List<Long> preamble = numbers.subList(index - preambleLength, index);
        Long sum = numbers.get(index);

        for (Long l : preamble) {
            long diff = sum - l;
            boolean contains = preamble.contains(diff);
            if(contains && diff != l){
                return true;
            }
        }
        return false;
    }
}