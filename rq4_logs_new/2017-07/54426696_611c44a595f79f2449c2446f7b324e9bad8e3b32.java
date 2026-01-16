package io.citrine.jcc.search.core.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.citrine.jcc.util.ListUtil;

import java.util.List;

/**
 * Definition of a data scope.
 * 
 * @author Kyle Michel
 */
public class DataScope {

    /**
     * Set the list of queries. This replaces any filters that are already present.
     *
     * @param query List of {@link DataQuery} objects.
     * @return This object.
     */
    public DataScope setQuery(final List<DataQuery> query) {
        this.query = query;
        return this;
    }

    /**
     * Add to the list of queries.
     *
     * @param query List of {@link DataQuery} objects.
     * @return This object.
     */
    @JsonIgnore
    public DataScope addQuery(final List<DataQuery> query) {
        this.query = ListUtil.add(query, this.query);
        return this;
    }

    /**
     * Add to the list of queries.
     *
     * @param query {@link DataQuery} object to add.
     * @return This object.
     */
    @JsonIgnore
    public DataScope addQuery(final DataQuery query) {
        this.query = ListUtil.add(query, this.query);
        return this;
    }

    /**
     * Get the number of queries.
     *
     * @return Number of queries.
     */
    @JsonIgnore
    public int queryLength() {
        return ListUtil.length(this.query);
    }

    /**
     * Get an iterable over the queries.
     *
     * @return {@link Iterable} of {@link DataQuery} objects.
     */
    @JsonIgnore
    public Iterable<DataQuery> query() {
        return ListUtil.iterable(this.query);
    }

    /**
     * Get the query at the input index.
     *
     * @param index Index of the query to get.
     * @return {@link DataQuery} at the input index.
     */
    @JsonIgnore
    public DataQuery getQuery(final int index) {
        return ListUtil.get(this.query, index);
    }

    /**
     * Get the list of queries.
     *
     * @return List of {@link DataQuery} objects.
     */
    public List<DataQuery> getQuery() {
        return this.query;
    }

    /** List of queries against the content of datasets. */
    private List<DataQuery> query;
}
