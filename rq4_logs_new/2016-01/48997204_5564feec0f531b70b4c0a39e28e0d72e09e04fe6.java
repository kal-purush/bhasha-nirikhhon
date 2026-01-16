package no.bacheloroppgaven.android.learningbasics1.model;

public class Note {

    private String name;
    private Double price;
    private Integer count;

    public Note(String name) {
        this(name, 1);
    }

    public Note(String name, Double price) {
        this(name, price, 1);
    }

    public Note(String name, Integer count) {
        this(name, 0.0, count);
    }

    public Note(String name, Double price, Integer count) {
        this.name = name;
        this.price = price;
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}