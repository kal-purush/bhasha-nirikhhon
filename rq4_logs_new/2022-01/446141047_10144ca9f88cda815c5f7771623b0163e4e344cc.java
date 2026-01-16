package com.mylottery.combinacao;

/**
 * This combination logic is credited to:
 * https://daemoniolabs.wordpress.com/2011/07/04/gerando-combinacoes-usando-java/amp/
 */
public class Combination {
    private int kElements ;
    private String[] nElements ;
    private int max ;
    private int n;

    /**
     * if kElements is zero, then calculate all possible combinations
     */
    public Combination(String[] nElements, int kElementsPerCombination) {
        this.kElements = kElementsPerCombination ;
        this.nElements = nElements ;
        this.max = ~(1 << nElements.length) ;
        this.n = 1;
    }

    /** 
     * Returns true when there is at least one combination
     */
    public boolean hasNext() {
        if ( kElements != 0 ) {
            while ( ((this.n & this.max) != 0) && (countbits() != kElements) ) n+=1 ;
        }

        return (this.n & this.max) != 0;
    }

    /** 
     * Returns the quantity of active bits of n
     */
    private int countbits() {
        int i;
        int c;

        i = 1;
        c = 0;
        while ( (this.max & i) != 0 ) {
            if ( (this.n & i) != 0) {
                c++;
            }
            i = i << 1 ;
        }

        return c ;
    }

    /**
     * Returns the length of each combination
     */
    private int getResultLength() {
        if (kElements != 0) {
            return kElements;
        }

        return this.countbits();
    }

    /**
     * Returns the next combination
     */
    public String[] nextCombination() {
        int result_index, enter_index, i;

        String[] combination = new String[this.getResultLength()];

        enter_index = 0;
        result_index = 0;
        i = 1;

        while ((this.max & i) != 0) {
            if ((this.n & i) != 0) {
                combination[result_index] = nElements[enter_index];
                result_index += 1;
            }
            enter_index += 1;
            i = i << 1;
        }

        n += 1;

        return combination;
    }
}