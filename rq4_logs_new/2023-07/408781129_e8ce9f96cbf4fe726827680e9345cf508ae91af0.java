package nl.recognize.dwh.application.model;

public class QueryBuilderOrder {
    private final String field;
    private final boolean ascending;

    public QueryBuilderOrder(String field, boolean ascending) {
        this.field = field;
        this.ascending = ascending;
    }

    public String getField() {
        return field;
    }

    public boolean isAscending() {
        return ascending;
    }
}