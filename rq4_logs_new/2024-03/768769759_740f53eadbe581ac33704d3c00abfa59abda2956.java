package bg.uni.fmi.lab01.baseline;

import java.util.Comparator;

public class Dog implements Comparable<Dog>{
    private String breed;
    private Integer weight;

    Dog(String breed,Integer weight){
        this.breed=breed;
        this.weight=weight;
    }

    public String getBreed() {
        return breed;
    }

    public void setBreed(String breed) {
        this.breed = breed;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    @Override
    public int compareTo(Dog o) {
        return this.weight-o.weight;
    }

    @Override
    public String toString() {
        return String.format("%s with %d",breed,weight);
    }
}
