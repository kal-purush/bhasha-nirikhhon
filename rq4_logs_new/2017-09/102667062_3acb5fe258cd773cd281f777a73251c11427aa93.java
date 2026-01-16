
package com.foo.umbrella.data.model;

import com.squareup.moshi.Json;

public class Dewpoint {

    @Json(name = "english")
    private String english;
    @Json(name = "metric")
    private String metric;

    /**
     * No args constructor for use in serialization
     * 
     */
    public Dewpoint() {
    }

    /**
     * 
     * @param metric
     * @param english
     */
    public Dewpoint(String english, String metric) {
        super();
        this.english = english;
        this.metric = metric;
    }

    public String getEnglish() {
        return english;
    }

    public void setEnglish(String english) {
        this.english = english;
    }

    public Dewpoint withEnglish(String english) {
        this.english = english;
        return this;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Dewpoint withMetric(String metric) {
        this.metric = metric;
        return this;
    }

}