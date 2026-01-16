package com.example.projectmrsisa.dto;

import java.util.List;

public class GraphDTO {
    private List<String> dates;
    private List<Integer> values;

    public GraphDTO(){

    }

    public GraphDTO(List<String> dates, List<Integer> values){
        this.dates = dates;
        this.values = values;
    }

    public List<String> getDates() {
        return dates;
    }

    public void setDates(List<String> dates) {
        this.dates = dates;
    }

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
    }
}