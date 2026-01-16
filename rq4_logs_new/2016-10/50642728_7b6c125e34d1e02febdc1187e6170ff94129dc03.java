package io.citrine.jpif.stats.common;

import java.util.List;

/**
 * Class used to store statistics about a single field in a PIF. This class tracks the total number of terms
 * encountered, number of numeric terms, min and max of the numeric terms, and the most commonly encountered terms.
 *
 * @author Kyle Michel
 */
public class FieldStatsWrapper extends StatsWrapper {

    /**
     * Set the number of numeric values found.
     *
     * @param numericCount Total number of numeric values found.
     */
    public void setNumericCount(final long numericCount) {
        this.numericCount = numericCount;
    }

    /**
     * Get the number of numeric values found.
     *
     * @return Number of numeric values found.
     */
    public long getNumericCount() {
        return this.numericCount;
    }

    /**
     * Set the minimum numeric value that was found.
     *
     * @param min Minimum numeric value that was found.
     */
    public void setMin(final Double min) {
        this.min = min;
    }

    /**
     * Get the minimum numeric value that was found.
     *
     * @return Minimum numeric value that was found.
     */
    public Double getMin() {
        return this.min;
    }

    /**
     * Set the maximum numeric value that was found.
     *
     * @param max Maximum numeric value that was found.
     */
    public void setMax(final Double max) {
        this.max = max;
    }

    /**
     * Get the maximum numeric value that was found.
     *
     * @return Maximum numeric value that was found.
     */
    public Double getMax() {
        return this.max;
    }

    /**
     * Set the common terms that were found in the field.
     *
     * @param commonTerms List of {@link TermAndCount} objects that were found.
     */
    public void setCommonTerms(final List<TermAndCount> commonTerms) {
        this.commonTerms = commonTerms;
    }

    /**
     * Get the common terms that were found.
     *
     * @return List of {@link TermAndCount} objects for the most common terms that were found.
     */
    public List<TermAndCount> getCommonTerms() {
        return this.commonTerms;
    }

    /** Number of numeric terms that were found. */
    private long numericCount;

    /** Minimum numeric value that was found. */
    private Double min;

    /** Maximum value that was found. */
    private Double max;

    /** List of common terms. */
    private List<TermAndCount> commonTerms;

    /**
     * Class to store statistics about a term and its count.
     *
     * @author Kyle Michel
     */
    public static class TermAndCount extends Stats {

        /**
         * Set the term.
         *
         * @param term String with the term to save.
         */
        public void setTerm(final String term) {
            this.term = term;
        }

        /**
         * Get the term stored by this object.
         *
         * @return String with the term for this object.
         */
        public String getTerm() {
            return this.term;
        }

        /** String with the term. */
        private String term;
    }
}