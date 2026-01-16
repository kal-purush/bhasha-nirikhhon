package com.github.missioncriticalcloud.cosmic.usage.core.model;

import com.fasterxml.jackson.annotation.JsonView;
import com.github.missioncriticalcloud.cosmic.usage.core.views.DetailedView;

public class Tag extends Resource {

    @JsonView(DetailedView.class)
    private String key;

    @JsonView(DetailedView.class)
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }
}