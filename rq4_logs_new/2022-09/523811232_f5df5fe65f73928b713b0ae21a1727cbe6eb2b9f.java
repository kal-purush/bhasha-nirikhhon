package com.tobiakindele.ehr.server.app.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Query{
    private Selector selector;
    private List<String> fields;
    private String bookmark;
    private int limit;

    public Selector getSelector() {
        return selector;
    }

    public void setSelector(Selector selector) {
        this.selector = selector;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getBookmark() {
        return bookmark;
    }

    public void setBookmark(String bookmark) {
        this.bookmark = bookmark;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public static class Not{
        @JsonProperty("id")
        private final Object objId;

        public Not(final Object objId) {
            this.objId = objId;
        }

        public Object getObjId() {
            return objId;
        }
    }

    public static class Selector{
        @JsonProperty("$not")
        private final Not not;

        public Selector(final Not not) {
            this.not = not;
        }

        public Not getNot() {
            return not;
        }
    }
}