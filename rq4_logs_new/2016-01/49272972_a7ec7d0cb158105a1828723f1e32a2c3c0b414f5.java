package pl.dgrebowiec.counterwords.domain;

import lombok.Data;

/**
 * @author Dariusz Grebowiec <dariusz.grebowiec@coi.gov.pl>
 */

@Data
public class CounterWord implements Comparable<CounterWord> {
    private final String word;
    private final long numberRepeat;

    private double percent;
    private String translateWord;

    @Override
    public int compareTo(CounterWord word) {
        return Long.compare(this.numberRepeat, word.getNumberRepeat());
    }

}